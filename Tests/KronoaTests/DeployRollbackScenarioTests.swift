import Testing
import Foundation
@testable import Kronoa

/// Simulates the Kronoa storage layout and demonstrates how a viewer session
/// sees content change through deploy, rollback, and hotfix cycles.
///
/// This test uses raw storage operations to simulate what ContentSession would do,
/// showing the viewer's perspective of content changes.
@Suite struct DeployRollbackScenarioTests {
    var storage: LocalFileStorage
    var tempDir: URL

    init() throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kronoa-scenario-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        storage = LocalFileStorage(root: tempDir)
    }

    // MARK: - Helpers to simulate Kronoa operations

    /// Simulate setup: create genesis edition and pointers
    private func simulateSetup() async throws {
        // Create .head counter
        _ = try await storage.atomicIncrement(path: "editions/.head", initialValue: 10000)

        // Create genesis edition (empty)
        try await storage.write(path: "editions/10000/.origin", data: Data())

        // Point both staging and production to genesis
        try await storage.write(
            path: ".production.json",
            data: #"{"edition": 10000}"#.data(using: .utf8)!
        )
        try await storage.write(
            path: ".staging.json",
            data: #"{"edition": 10000}"#.data(using: .utf8)!
        )
    }

    /// Simulate writing content to an edition
    private func writeContent(edition: Int, path: String, content: String) async throws {
        // Compute hash (simplified - just use content as "hash" for testing)
        let hash = content.data(using: .utf8)!.base64EncodedString()

        // Write object
        let shard = String(hash.prefix(2))
        try await storage.write(
            path: "objects/\(shard)/\(hash).dat",
            data: content.data(using: .utf8)!
        )

        // Write path mapping
        try await storage.write(
            path: "editions/\(edition)/\(path)",
            data: "sha256:\(hash)".data(using: .utf8)!
        )
    }

    /// Simulate reading content as a viewer (follows edition's path -> object)
    private func readAsViewer(edition: Int, path: String) async throws -> String {
        // Read path mapping from edition
        let mappingData = try await storage.read(path: "editions/\(edition)/\(path)")
        let mapping = String(data: mappingData, encoding: .utf8)!

        // Extract hash
        let hash = mapping.replacingOccurrences(of: "sha256:", with: "")

        // Read object
        let shard = String(hash.prefix(2))
        let objectData = try await storage.read(path: "objects/\(shard)/\(hash).dat")

        return String(data: objectData, encoding: .utf8)!
    }

    /// Get current production edition
    private func getProductionEdition() async throws -> Int {
        let data = try await storage.read(path: ".production.json")
        let json = try JSONDecoder().decode([String: Int].self, from: data)
        return json["edition"]!
    }

    /// Simulate deploy: copy staging to production
    private func simulateDeploy() async throws {
        let lock = try await storage.acquireLock(path: ".lock", timeout: 5, leaseDuration: 30)
        do {
            let stagingData = try await storage.read(path: ".staging.json")
            try await storage.write(path: ".production.json", data: stagingData)
            try await lock.release()
        } catch {
            try? await lock.release()
            throw error
        }
    }

    /// Simulate staging an edition
    private func simulateStage(edition: Int) async throws {
        let lock = try await storage.acquireLock(path: ".lock", timeout: 5, leaseDuration: 30)
        do {
            try await storage.write(
                path: ".staging.json",
                data: #"{"edition": \#(edition)}"#.data(using: .utf8)!
            )
            try await lock.release()
        } catch {
            try? await lock.release()
            throw error
        }
    }

    /// Simulate rollback: point staging to a previous edition
    private func simulateRollbackStaging(to edition: Int) async throws {
        try await simulateStage(edition: edition)
    }

    // MARK: - Scenario Tests

    @Test func viewerSeesContentChangeThroughDeployRollbackHotfix() async throws {
        // ===== SETUP =====
        try await simulateSetup()

        // ===== EDITION 10001: Initial content =====
        _ = try await storage.atomicIncrement(path: "editions/.head", initialValue: 10000)
        try await storage.write(path: "editions/10001/.origin", data: "10000".data(using: .utf8)!)
        try await writeContent(edition: 10001, path: "article.txt", content: "Version 1: Hello World")

        // Stage and deploy 10001
        try await simulateStage(edition: 10001)
        try await simulateDeploy()

        // ----- Viewer Session A: sees Version 1 -----
        let viewerEditionA = try await getProductionEdition()
        #expect(viewerEditionA == 10001)

        let contentA = try await readAsViewer(edition: viewerEditionA, path: "article.txt")
        #expect(contentA == "Version 1: Hello World")

        // ===== EDITION 10002: Update (will be buggy) =====
        _ = try await storage.atomicIncrement(path: "editions/.head", initialValue: 10000)
        try await storage.write(path: "editions/10002/.origin", data: "10001".data(using: .utf8)!)
        try await writeContent(edition: 10002, path: "article.txt", content: "Version 2: BUGGY CONTENT!")

        // Stage and deploy 10002
        try await simulateStage(edition: 10002)
        try await simulateDeploy()

        // ----- Viewer Session B: sees buggy Version 2 -----
        let viewerEditionB = try await getProductionEdition()
        #expect(viewerEditionB == 10002)

        let contentB = try await readAsViewer(edition: viewerEditionB, path: "article.txt")
        #expect(contentB == "Version 2: BUGGY CONTENT!")

        // ===== ROLLBACK to 10001 =====
        try await simulateRollbackStaging(to: 10001)
        try await simulateDeploy()

        // ----- Viewer Session C: sees Version 1 again (rolled back) -----
        let viewerEditionC = try await getProductionEdition()
        #expect(viewerEditionC == 10001)

        let contentC = try await readAsViewer(edition: viewerEditionC, path: "article.txt")
        #expect(contentC == "Version 1: Hello World")

        // ===== EDITION 10003: Hotfix =====
        _ = try await storage.atomicIncrement(path: "editions/.head", initialValue: 10000)
        try await storage.write(path: "editions/10003/.origin", data: "10001".data(using: .utf8)!)
        try await writeContent(edition: 10003, path: "article.txt", content: "Version 3: Hello World (with hotfix)")

        // Stage and deploy 10003
        try await simulateStage(edition: 10003)
        try await simulateDeploy()

        // ----- Viewer Session D: sees hotfixed Version 3 -----
        let viewerEditionD = try await getProductionEdition()
        #expect(viewerEditionD == 10003)

        let contentD = try await readAsViewer(edition: viewerEditionD, path: "article.txt")
        #expect(contentD == "Version 3: Hello World (with hotfix)")
    }

    @Test func concurrentViewersSeeSameContent() async throws {
        // Setup
        try await simulateSetup()

        // Create and deploy edition 10001
        _ = try await storage.atomicIncrement(path: "editions/.head", initialValue: 10000)
        try await storage.write(path: "editions/10001/.origin", data: "10000".data(using: .utf8)!)
        try await writeContent(edition: 10001, path: "data.json", content: #"{"value": 42}"#)
        try await simulateStage(edition: 10001)
        try await simulateDeploy()

        // Simulate multiple concurrent viewers reading the same content
        for _ in 0..<5 {
            let edition = try await getProductionEdition()
            let content = try await readAsViewer(edition: edition, path: "data.json")
            #expect(edition == 10001)
            #expect(content == #"{"value": 42}"#)
        }
    }

    @Test func viewerIsolationDuringDeploy() async throws {
        // Setup
        try await simulateSetup()

        // Create and deploy edition 10001
        _ = try await storage.atomicIncrement(path: "editions/.head", initialValue: 10000)
        try await storage.write(path: "editions/10001/.origin", data: "10000".data(using: .utf8)!)
        try await writeContent(edition: 10001, path: "config.txt", content: "Config V1")
        try await simulateStage(edition: 10001)
        try await simulateDeploy()

        // Viewer starts a "session" - captures the current production edition
        let viewerSessionEdition = try await getProductionEdition()
        #expect(viewerSessionEdition == 10001)

        // Meanwhile, admin creates and deploys edition 10002
        _ = try await storage.atomicIncrement(path: "editions/.head", initialValue: 10000)
        try await storage.write(path: "editions/10002/.origin", data: "10001".data(using: .utf8)!)
        try await writeContent(edition: 10002, path: "config.txt", content: "Config V2")
        try await simulateStage(edition: 10002)
        try await simulateDeploy()

        // New production is 10002
        let newProduction = try await getProductionEdition()
        #expect(newProduction == 10002)

        // But the original viewer session still reads from 10001 (session isolation)
        let viewerContent = try await readAsViewer(edition: viewerSessionEdition, path: "config.txt")
        #expect(viewerContent == "Config V1")

        // A new viewer would see 10002
        let newViewerEdition = try await getProductionEdition()
        let newViewerContent = try await readAsViewer(edition: newViewerEdition, path: "config.txt")
        #expect(newViewerContent == "Config V2")
    }
}
