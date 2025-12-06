import ArgumentParser
import Foundation
import Kronoa

// MARK: - Pending Command

struct Pending: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "List pending submissions"
    )

    @Flag(name: .long, help: "Output as JSON")
    var json = false

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        let pending = try await session.listPending()

        if json {
            let encoder = JSONEncoder()
            encoder.outputFormatting = .prettyPrinted
            encoder.dateEncodingStrategy = .iso8601
            let data = try encoder.encode(pending)
            print(String(data: data, encoding: .utf8) ?? "[]")
        } else {
            if pending.isEmpty {
                print("No pending submissions")
            } else {
                for p in pending {
                    print("\(p.edition): \(p.message) [\(p.label)]")
                }
            }
        }
    }
}

// MARK: - Stage Command

struct Stage: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Accept submission into staging"
    )

    @Argument(help: "Edition ID to stage")
    var edition: Int

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        try await session.stage(edition: edition)
        print("Staged edition \(edition)")
    }
}

// MARK: - Reject Command

struct Reject: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Reject submission"
    )

    @Argument(help: "Edition ID to reject")
    var edition: Int

    @Argument(help: "Rejection reason")
    var reason: String

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        try await session.reject(edition: edition, reason: reason)
        print("Rejected edition \(edition): \(reason)")
    }
}

// MARK: - Rejected Command

struct Rejected: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "List or get rejected submissions"
    )

    @Argument(help: "Edition ID (optional, list all if omitted)")
    var edition: Int?

    @Flag(name: .long, help: "Output as JSON")
    var json = false

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        if let edition = edition {
            // Get specific rejection
            if let rejection = try await session.getRejection(edition: edition) {
                if json {
                    let encoder = JSONEncoder()
                    encoder.outputFormatting = .prettyPrinted
                    encoder.dateEncodingStrategy = .iso8601
                    let data = try encoder.encode(rejection)
                    print(String(data: data, encoding: .utf8) ?? "{}")
                } else {
                    print("Edition:  \(rejection.edition)")
                    print("Reason:   \(rejection.reason)")
                    print("Rejected: \(rejection.rejectedAt)")
                }
            } else {
                print("No rejection found for edition \(edition)")
            }
        } else {
            // List all rejections
            let rejections = try await session.listRejected()

            if json {
                let encoder = JSONEncoder()
                encoder.outputFormatting = .prettyPrinted
                encoder.dateEncodingStrategy = .iso8601
                let data = try encoder.encode(rejections)
                print(String(data: data, encoding: .utf8) ?? "[]")
            } else {
                if rejections.isEmpty {
                    print("No rejected submissions")
                } else {
                    for r in rejections {
                        print("\(r.edition): \(r.reason)")
                    }
                }
            }
        }
    }
}

// MARK: - Deploy Command

struct Deploy: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Promote staging to production"
    )

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        try await session.deploy()
        print("Deployed staging to production")
    }
}

// MARK: - AdminRollback Command

struct AdminRollback: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "admin-rollback",
        abstract: "Rollback to a previous edition"
    )

    @Argument(help: "Edition ID to rollback to")
    var edition: Int

    @Flag(name: .long, help: "Only set staging pointer, skip deploy")
    var noDeploy = false

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        try await session.setStagingPointer(to: edition)
        print("Set staging to edition \(edition)")

        if !noDeploy {
            try await session.deploy()
            print("Deployed to production")
        }
    }
}

// MARK: - Flatten Command

struct Flatten: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Flatten edition (collapse ancestry)"
    )

    @Argument(help: "Edition ID to flatten")
    var edition: Int

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        try await session.flatten(edition: edition)
        print("Flattened edition \(edition)")
    }
}

// MARK: - Gc Command

struct Gc: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Garbage collection"
    )

    @Flag(name: .long, help: "List orphaned objects (required)")
    var list = false

    @Flag(name: .long, help: "Output as JSON")
    var json = false

    func run() async throws {
        guard list else {
            print("Error: --list is required")
            print("Usage: kronoa gc --list")
            print("")
            print("Note: Only --list is currently available.")
            print("Actual deletion (--execute) requires mtime support which is not yet implemented.")
            throw ExitCode.failure
        }

        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .staging)

        let result = try await session.gc(dryRun: true)

        if json {
            let output: [String: Int] = [
                "scannedObjects": result.scannedObjects,
                "deletedObjects": result.deletedObjects,
                "skippedByRef": result.skippedByRef,
                "skippedByScan": result.skippedByScan,
                "skippedByAge": result.skippedByAge,
                "errors": result.errors
            ]
            if let data = try? JSONSerialization.data(withJSONObject: output, options: .prettyPrinted),
               let str = String(data: data, encoding: .utf8) {
                print(str)
            }
        } else {
            print("GC Analysis (dry-run):")
            print("  Scanned:        \(result.scannedObjects)")
            print("  Live (ref):     \(result.skippedByRef)")
            print("  Live (scan):    \(result.skippedByScan)")
            print("  Orphaned:       \(result.skippedByAge)")
            print("  Errors:         \(result.errors)")
        }
    }
}
