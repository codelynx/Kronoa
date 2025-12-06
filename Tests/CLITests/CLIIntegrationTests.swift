import Foundation
import Testing
@testable import KronoaCLI
@testable import Kronoa

/// Integration tests for Kronoa CLI using story-based scenarios.
///
/// These tests simulate CLI workflows by using the same helper functions
/// and session management the CLI uses, validating end-to-end behavior.
///
/// ## Test Stories
///
/// Each test represents a realistic user workflow:
///
/// 1. **Alice publishes a blog post** - Full editor workflow
///    - Check status → checkout → write file → list/read → submit → stage → deploy → verify production
///
/// 2. **Bob updates atomically** - Transaction support
///    - Setup files → checkout → beginEditing → multi-file write → endEditing → submit → verify
///
/// 3. **Carol's submission rejected** - Rejection workflow
///    - Checkout → write → submit → admin rejects → query rejection reason → list rejections
///
/// 4. **Admin rollback** - Emergency recovery
///    - Deploy good content → deploy bad content → admin-rollback to good → verify restored
///
/// 5. **Dave discards mistake** - Undo uncommitted changes
///    - Setup file → checkout → write wrong data → discard → verify original → write correct
///
/// 6. **Eve explores content** - Navigation and listing
///    - Setup directory structure → cd to directory → list files → cd to root → stat file
///
/// 7. **Frank submitted mode blocked** - Post-submit behavior
///    - Submit work → verify getSessionMode throws → clear session → browse staging again
///
/// ## Coverage Gap
///
/// These tests exercise the core session logic and CLI helper functions but do NOT:
/// - Execute the actual `kronoa` binary
/// - Test ArgumentParser flag parsing (e.g., `--json`, `--glob`, `gc --list`)
/// - Verify CLI output formatting
/// - Test error messages displayed to users
///
/// For full end-to-end CLI coverage, additional tests would need to:
/// - Spawn the executable via `Process` and capture stdout/stderr
/// - Or use ArgumentParser's testing utilities to drive commands directly
///
/// ## Serialization
///
/// All tests run serially because they change the process-global current working
/// directory, which would cause race conditions if run in parallel.

// MARK: - Test Helpers

/// Test environment that sets up isolated storage and config for each test.
final class CLITestEnvironment {
    let tempDir: URL
    let storageDir: URL
    let configDir: URL
    let originalDir: String

    init() throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kronoa-cli-test-\(UUID().uuidString)")
        storageDir = tempDir.appendingPathComponent("storage")
        configDir = tempDir.appendingPathComponent("workspace")

        try FileManager.default.createDirectory(at: storageDir, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: configDir, withIntermediateDirectories: true)

        // Save original directory and switch to test workspace
        originalDir = FileManager.default.currentDirectoryPath
        FileManager.default.changeCurrentDirectoryPath(configDir.path)
    }

    /// Initialize storage with genesis edition.
    func setupStorage() async throws -> LocalFileStorage {
        let storage = LocalFileStorage(root: storageDir)

        // Create genesis edition 10000
        try await storage.write(
            path: "contents/editions/.head",
            data: "10000".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/.production.json",
            data: "{\"edition\":10000}".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/.staging.json",
            data: "{\"edition\":10000}".data(using: .utf8)!
        )

        // Configure CLI to use this storage
        var config = SessionConfig()
        config.storage = "file://\(storageDir.path)"
        try config.save()

        return storage
    }

    /// Create a file in the local workspace for upload.
    func createLocalFile(_ name: String, content: String) throws -> URL {
        let file = configDir.appendingPathComponent(name)
        try content.write(to: file, atomically: true, encoding: .utf8)
        return file
    }

    /// Simulate checkout command.
    func checkout(label: String, from source: CheckoutSource = .staging) async throws {
        var config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        try await session.checkout(label: label, from: source)

        let editionId = await session.editionId

        config.mode = "editing"
        config.label = label
        config.edition = editionId
        try config.save()
    }

    /// Simulate submit command.
    func submit(message: String) async throws {
        var config = SessionConfig.load()
        guard config.mode == "editing", let label = config.label else {
            throw CLIError.notInEditingMode
        }

        let storage = try await createStorageBackend(from: config)
        let session = try await ContentSession(storage: storage, mode: .editing(label: label))
        try await session.submit(message: message)

        config.mode = "submitted"
        try config.save()
    }

    /// Simulate stage command.
    func stage(edition: Int) async throws {
        let config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.stage(edition: edition)
    }

    /// Simulate deploy command.
    func deploy() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.deploy()
    }

    /// Simulate reject command.
    func reject(edition: Int, reason: String) async throws {
        let config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.reject(edition: edition, reason: reason)
    }

    /// Simulate admin-rollback command.
    func adminRollback(edition: Int, deployAfter: Bool = true) async throws {
        let config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.setStagingPointer(to: edition)
        if deployAfter {
            try await session.deploy()
        }
    }

    /// Simulate discard command.
    func discard(path: String) async throws {
        let config = SessionConfig.load()
        guard config.mode == "editing", let label = config.label else {
            throw CLIError.notInEditingMode
        }

        let storage = try await createStorageBackend(from: config)
        let session = try await ContentSession(storage: storage, mode: .editing(label: label))

        // Parse kr: path
        let remotePath: String
        if path.hasPrefix("kr:") {
            remotePath = String(path.dropFirst(3))
        } else {
            remotePath = path
        }

        try await session.discard(path: remotePath)
    }

    /// Simulate cd command.
    func cd(path: String) throws {
        var config = SessionConfig.load()

        let newPath: String
        if path == "/" {
            newPath = "/"
        } else if path == ".." {
            let current = config.cwd ?? "/"
            let components = current.split(separator: "/").dropLast()
            newPath = "/" + components.joined(separator: "/") + (components.isEmpty ? "" : "/")
        } else {
            var target = path
            if target.hasPrefix("kr:") {
                target = String(target.dropFirst(3))
            }
            if !target.hasPrefix("/") {
                let current = config.cwd ?? "/"
                target = current + target
            }
            if !target.hasSuffix("/") {
                target += "/"
            }
            newPath = target
        }

        config.cwd = newPath
        try config.save()
    }

    /// Clear editing mode from config.
    func clearEditingMode() throws {
        var config = SessionConfig.load()
        config.mode = nil
        config.label = nil
        config.edition = nil
        try config.save()
    }

    /// Create storage backend from config.
    private func createStorageBackend(from config: SessionConfig) async throws -> StorageBackend {
        guard let storageUrl = config.storage else {
            throw CLIError.noStorageConfigured
        }
        // For tests, we know it's file://
        let path = String(storageUrl.dropFirst(7))
        return LocalFileStorage(root: URL(fileURLWithPath: path))
    }

    /// Get current session for reading/writing.
    func getSession() async throws -> ContentSession {
        let config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        let mode = try getSessionMode(from: config)
        return try await ContentSession(storage: storage, mode: mode)
    }

    /// Get staging session.
    func getStagingSession() async throws -> ContentSession {
        let config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        return try await ContentSession(storage: storage, mode: .staging)
    }

    /// Get production session.
    func getProductionSession() async throws -> ContentSession {
        let config = SessionConfig.load()
        let storage = try await createStorageBackend(from: config)
        return try await ContentSession(storage: storage, mode: .production)
    }

    func cleanup() {
        FileManager.default.changeCurrentDirectoryPath(originalDir)
        try? FileManager.default.removeItem(at: tempDir)
    }
}

// MARK: - CLI Integration Tests

/// All CLI story tests in one serialized suite to avoid parallel execution conflicts.
@Suite("CLI Integration Stories", .serialized)
struct CLIIntegrationStories {

    // MARK: - Story 1: Editor Publishing Workflow

    /// **Story 1: Alice publishes a new blog post**
    ///
    /// Alice is a content editor who wants to publish a new blog post.
    ///
    /// Steps:
    /// 1. Alice checks status - storage is configured, no active mode
    /// 2. Alice checks out "alice-blog-post" - enters editing mode, gets edition 10001
    /// 3. Alice writes her blog post to articles/my-post.md
    /// 4. Alice reviews - lists files, reads back content
    /// 5. Alice submits for review - mode becomes "submitted"
    /// 6. Admin clears editing mode, lists pending, stages edition 10001, deploys
    /// 7. Verify: content is readable from production
    @Test("Story 1: Alice publishes a new blog post")
    func alicePublishesBlogPost() async throws {
        let env = try CLITestEnvironment()
        defer { env.cleanup() }
        _ = try await env.setupStorage()

        // Step 1: Check status - should show storage configured
        let config1 = SessionConfig.load()
        #expect(config1.storage != nil)
        #expect(config1.mode == nil)

        // Step 2: Alice checks out to start editing
        try await env.checkout(label: "alice-blog-post")

        let config2 = SessionConfig.load()
        #expect(config2.mode == "editing")
        #expect(config2.label == "alice-blog-post")
        #expect(config2.edition == 10001)

        // Step 3: Alice writes her blog post
        let postContent = """
        # My First Blog Post

        Hello world! This is my first post.
        """

        let session = try await env.getSession()
        try await session.write(path: "articles/my-post.md", data: postContent.data(using: .utf8)!)

        // Step 4: Alice reviews - list files and read back
        let files = try await session.list(directory: "articles")
        #expect(files.contains("my-post.md"))

        let readBack = try await session.read(path: "articles/my-post.md")
        #expect(String(data: readBack, encoding: .utf8) == postContent)

        // Step 5: Alice submits for review
        try await env.submit(message: "Add my first blog post")

        let config3 = SessionConfig.load()
        #expect(config3.mode == "submitted")

        // Step 6: Admin reviews and stages
        try env.clearEditingMode()

        let adminSession = try await env.getStagingSession()
        let pending = try await adminSession.listPending()
        #expect(pending.count == 1)
        #expect(pending[0].edition == 10001)
        #expect(pending[0].message == "Add my first blog post")

        // Stage and deploy
        try await env.stage(edition: 10001)
        try await env.deploy()

        // Verify: read from production
        let prodSession = try await env.getProductionSession()
        let prodContent = try await prodSession.read(path: "articles/my-post.md")
        #expect(String(data: prodContent, encoding: .utf8) == postContent)
    }

    // MARK: - Story 2: Content Update with Transaction

    /// **Story 2: Bob updates config and version atomically**
    ///
    /// Bob needs to update multiple related files (config.json and version.txt)
    /// atomically so they're always in sync.
    ///
    /// Steps:
    /// 1. Setup: Create initial config.json (v1.0) and version.txt (1.0.0), stage
    /// 2. Bob checks out "bob-update"
    /// 3. Bob starts transaction with beginEditing()
    /// 4. Bob updates both files (buffered, not yet written)
    /// 5. Bob commits transaction with endEditing() - both written atomically
    /// 6. Bob submits, admin stages edition 10002, deploys
    /// 7. Verify: both files show v2.0 in production
    @Test("Story 2: Bob updates config and version atomically")
    func bobUpdatesAtomically() async throws {
        let env = try CLITestEnvironment()
        defer { env.cleanup() }
        _ = try await env.setupStorage()

        // Setup: Create initial files in staging
        try await env.checkout(label: "setup-bob")

        let setupSession = try await env.getSession()
        try await setupSession.write(path: "config.json", data: "{\"version\": \"1.0\"}".data(using: .utf8)!)
        try await setupSession.write(path: "version.txt", data: "1.0.0".data(using: .utf8)!)
        try await env.submit(message: "Initial setup")

        try env.clearEditingMode()
        try await env.stage(edition: 10001)

        // Now Bob starts his update
        try await env.checkout(label: "bob-update")

        let bobSession = try await env.getSession()

        // Start transaction
        try await bobSession.beginEditing()

        // Update both files (buffered in transaction)
        try await bobSession.write(path: "config.json", data: "{\"version\": \"2.0\", \"features\": [\"new\"]}".data(using: .utf8)!)
        try await bobSession.write(path: "version.txt", data: "2.0.0".data(using: .utf8)!)

        // Commit transaction
        try await bobSession.endEditing()

        // Submit
        try await env.submit(message: "Upgrade to v2.0")

        // Admin stages and deploys
        try env.clearEditingMode()
        try await env.stage(edition: 10002)
        try await env.deploy()

        // Verify both files updated in production
        let prodSession = try await env.getProductionSession()
        let configContent = try await prodSession.read(path: "config.json")
        let versionContent = try await prodSession.read(path: "version.txt")

        #expect(String(data: configContent, encoding: .utf8)!.contains("2.0"))
        #expect(String(data: versionContent, encoding: .utf8) == "2.0.0")
    }

    // MARK: - Story 3: Rejected Submission

    /// **Story 3: Carol's submission is rejected with feedback**
    ///
    /// Carol submits a draft post, but the admin rejects it with feedback.
    ///
    /// Steps:
    /// 1. Carol checks out "carol-draft"
    /// 2. Carol writes articles/draft.md with incomplete content
    /// 3. Carol submits "New draft article"
    /// 4. Admin rejects edition 10001 with reason "Please add more content..."
    /// 5. Verify: getRejection(10001) returns the rejection with reason
    /// 6. Verify: listRejected() includes edition 10001
    @Test("Story 3: Carol's submission is rejected with feedback")
    func carolSubmissionRejected() async throws {
        let env = try CLITestEnvironment()
        defer { env.cleanup() }
        _ = try await env.setupStorage()

        // Carol creates and submits
        try await env.checkout(label: "carol-draft")

        let session = try await env.getSession()
        try await session.write(path: "articles/draft.md", data: "# Draft\n\nThis needs work.".data(using: .utf8)!)

        try await env.submit(message: "New draft article")

        // Admin rejects
        try env.clearEditingMode()
        try await env.reject(edition: 10001, reason: "Please add more content and fix formatting")

        // Verify rejection stored
        let adminSession = try await env.getStagingSession()

        let rejection = try await adminSession.getRejection(edition: 10001)
        #expect(rejection != nil)
        #expect(rejection?.reason == "Please add more content and fix formatting")

        // List all rejections
        let rejections = try await adminSession.listRejected()
        #expect(rejections.count == 1)
        #expect(rejections[0].edition == 10001)
    }

    // MARK: - Story 4: Emergency Rollback

    /// **Story 4: Admin rolls back bad deployment**
    ///
    /// A bad deployment makes it to production. Admin needs to quickly
    /// rollback to a known-good edition.
    ///
    /// Steps:
    /// 1. Deploy edition 10001 with good content: "<h1>Welcome</h1>"
    /// 2. Verify good content in production
    /// 3. Deploy edition 10002 with bad content: "<h1>BROKEN PAGE</h1>"
    /// 4. Verify bad content is now in production (oops!)
    /// 5. EMERGENCY: Admin runs adminRollback(10001) - sets staging + deploys
    /// 6. Verify: production now shows good content again
    @Test("Story 4: Admin rolls back bad deployment")
    func adminRollsBackBadDeployment() async throws {
        let env = try CLITestEnvironment()
        defer { env.cleanup() }
        _ = try await env.setupStorage()

        // Edition 10001: Good content
        try await env.checkout(label: "good-content")
        let session1 = try await env.getSession()
        try await session1.write(path: "index.html", data: "<h1>Welcome</h1>".data(using: .utf8)!)
        try await env.submit(message: "Good homepage")

        try env.clearEditingMode()
        try await env.stage(edition: 10001)
        try await env.deploy()

        // Verify good content in production
        let prodSession1 = try await env.getProductionSession()
        let goodContent = try await prodSession1.read(path: "index.html")
        #expect(String(data: goodContent, encoding: .utf8) == "<h1>Welcome</h1>")

        // Edition 10002: Bad content (oops!)
        try await env.checkout(label: "bad-content")
        let session2 = try await env.getSession()
        try await session2.write(path: "index.html", data: "<h1>BROKEN PAGE</h1>".data(using: .utf8)!)
        try await env.submit(message: "Update homepage")

        try env.clearEditingMode()
        try await env.stage(edition: 10002)
        try await env.deploy()

        // Verify bad content is now in production
        let prodSession2 = try await env.getProductionSession()
        let badContent = try await prodSession2.read(path: "index.html")
        #expect(String(data: badContent, encoding: .utf8) == "<h1>BROKEN PAGE</h1>")

        // EMERGENCY: Admin rolls back to edition 10001
        try await env.adminRollback(edition: 10001)

        // Verify rollback successful
        let prodSession3 = try await env.getProductionSession()
        let restoredContent = try await prodSession3.read(path: "index.html")
        #expect(String(data: restoredContent, encoding: .utf8) == "<h1>Welcome</h1>")
    }

    // MARK: - Story 5: Discard and Redo

    /// **Story 5: Dave discards mistake and redoes correctly**
    ///
    /// Dave makes a mistake while editing and needs to discard the
    /// uncommitted change before making the correct update.
    ///
    /// Steps:
    /// 1. Setup: Create data.txt with "original" content, stage
    /// 2. Dave checks out "dave-edit"
    /// 3. Dave writes "WRONG DATA" to data.txt (mistake!)
    /// 4. Dave reads back and sees the wrong data
    /// 5. Dave discards the change with discard("data.txt")
    /// 6. Verify: data.txt is back to "original"
    /// 7. Dave writes "correct data"
    /// 8. Dave submits - mode becomes "submitted"
    @Test("Story 5: Dave discards mistake and redoes correctly")
    func daveDiscardsAndRedoes() async throws {
        let env = try CLITestEnvironment()
        defer { env.cleanup() }
        _ = try await env.setupStorage()

        // Setup: Create existing file
        try await env.checkout(label: "setup-dave")
        let setupSession = try await env.getSession()
        try await setupSession.write(path: "data.txt", data: "original".data(using: .utf8)!)
        try await env.submit(message: "Setup")

        try env.clearEditingMode()
        try await env.stage(edition: 10001)

        // Dave starts editing
        try await env.checkout(label: "dave-edit")

        let daveSession = try await env.getSession()

        // Dave makes a mistake
        try await daveSession.write(path: "data.txt", data: "WRONG DATA".data(using: .utf8)!)

        // Dave reads back and realizes the mistake
        let wrongData = try await daveSession.read(path: "data.txt")
        #expect(String(data: wrongData, encoding: .utf8) == "WRONG DATA")

        // Dave discards the change
        try await env.discard(path: "kr:data.txt")

        // Verify data.txt is back to original (need fresh session to see ancestry)
        let freshSession = try await env.getSession()
        let restoredData = try await freshSession.read(path: "data.txt")
        #expect(String(data: restoredData, encoding: .utf8) == "original")

        // Dave makes the correct change
        try await freshSession.write(path: "data.txt", data: "correct data".data(using: .utf8)!)

        // Submit
        try await env.submit(message: "Update data correctly")

        // Verify submission
        let config = SessionConfig.load()
        #expect(config.mode == "submitted")
    }

    // MARK: - Story 6: Navigation and Listing

    /// **Story 6: Eve explores content repository**
    ///
    /// Eve wants to browse the content repository, navigate directories,
    /// list files, and inspect file metadata.
    ///
    /// Steps:
    /// 1. Setup: Create directory structure with articles/, images/, config.json
    /// 2. Eve checks initial cwd (should be "/" or nil)
    /// 3. Eve runs cd("kr:articles/") - cwd becomes "/articles/"
    /// 4. Eve lists articles directory - sees post1.md, post2.md
    /// 5. Eve runs cd("/") - cwd becomes "/"
    /// 6. Eve lists root - sees articles/, images/, config.json
    /// 7. Eve stats config.json - sees exists, size=2
    @Test("Story 6: Eve explores content repository")
    func eveExploresContent() async throws {
        let env = try CLITestEnvironment()
        defer { env.cleanup() }
        _ = try await env.setupStorage()

        // Setup: Create directory structure with files
        try await env.checkout(label: "setup-eve")
        let setupSession = try await env.getSession()

        try await setupSession.write(path: "articles/post1.md", data: "# Post 1".data(using: .utf8)!)
        try await setupSession.write(path: "articles/post2.md", data: "# Post 2".data(using: .utf8)!)
        try await setupSession.write(path: "images/logo.png", data: Data([0x89, 0x50, 0x4E, 0x47]))
        try await setupSession.write(path: "config.json", data: "{}".data(using: .utf8)!)

        try await env.submit(message: "Setup content")

        try env.clearEditingMode()
        try await env.stage(edition: 10001)

        // Eve checks initial cwd
        let config1 = SessionConfig.load()
        #expect(config1.cwd == nil || config1.cwd == "/")

        // Eve changes to articles directory
        try env.cd(path: "kr:articles/")

        let config2 = SessionConfig.load()
        #expect(config2.cwd == "/articles/")

        // Eve lists files in articles directory
        let eveSession = try await env.getStagingSession()

        let articleFiles = try await eveSession.list(directory: "articles")
        #expect(articleFiles.contains("post1.md"))
        #expect(articleFiles.contains("post2.md"))
        #expect(articleFiles.count == 2)

        // Eve goes back to root
        try env.cd(path: "/")

        let config3 = SessionConfig.load()
        #expect(config3.cwd == "/")

        // Eve lists root - should see articles/, images/, config.json
        let rootFiles = try await eveSession.list(directory: "")
        #expect(rootFiles.contains("articles/"))
        #expect(rootFiles.contains("images/"))
        #expect(rootFiles.contains("config.json"))

        // Eve stats a file
        let stat = try await eveSession.stat(path: "config.json")
        #expect(stat.status == .exists)
        #expect(stat.size == 2) // "{}"
    }

    // MARK: - Story 7: Submitted Mode Rejection

    /// **Story 7: Frank cannot browse after submitting until session cleared**
    ///
    /// After submitting, Frank tries to use CLI commands but they should
    /// be blocked until he clears the session.
    ///
    /// Steps:
    /// 1. Frank checks out "frank-work"
    /// 2. Frank writes work.txt and submits
    /// 3. Verify: config.mode == "submitted"
    /// 4. Frank tries getSessionMode() - should throw CLIError.submittedMode
    /// 5. Frank clears session with SessionConfig.clear()
    /// 6. Verify: config.mode == nil
    /// 7. Frank re-sets storage URL (clear removes everything)
    /// 8. Frank can now get staging session and list pending
    @Test("Story 7: Frank cannot browse after submitting until session cleared")
    func frankSubmittedModeBlocked() async throws {
        let env = try CLITestEnvironment()
        defer { env.cleanup() }
        _ = try await env.setupStorage()

        // Frank creates and submits
        try await env.checkout(label: "frank-work")
        let session = try await env.getSession()
        try await session.write(path: "work.txt", data: "work".data(using: .utf8)!)
        try await env.submit(message: "Frank's work")

        // Frank tries to get session - should fail
        let config = SessionConfig.load()
        #expect(config.mode == "submitted")

        #expect(throws: CLIError.self) {
            _ = try getSessionMode(from: config)
        }

        // Frank clears session
        try SessionConfig.clear()

        // Frank can now browse staging
        let newConfig = SessionConfig.load()
        #expect(newConfig.mode == nil)

        // Need to re-set storage since clear removes everything
        var freshConfig = SessionConfig()
        freshConfig.storage = "file://\(env.storageDir.path)"
        try freshConfig.save()

        // Frank can get a staging session now
        let stagingSession = try await env.getStagingSession()
        let pending = try await stagingSession.listPending()
        #expect(pending.count == 1)
    }
}
