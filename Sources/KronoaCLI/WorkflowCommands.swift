import ArgumentParser
import Foundation
import Kronoa

// MARK: - Checkout Command

struct Checkout: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Create working edition"
    )

    @Argument(help: "Working label")
    var label: String

    @Option(name: .long, help: "Branch from (staging or production)")
    var from: String = "staging"

    func run() async throws {
        var config = SessionConfig.load()
        let storage = try await createStorage(from: config)

        let source: CheckoutSource = from == "production" ? .production : .staging
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: label, from: source)

        let editionId = await session.editionId

        config.mode = "editing"
        config.label = label
        config.edition = editionId
        try config.save()

        print("Checked out edition \(editionId) as '\(label)' (from \(from))")
    }
}

// MARK: - Discard Command

struct Discard: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Discard uncommitted change"
    )

    @Argument(help: "Remote path (kr:path)")
    var path: String

    func run() async throws {
        let config = SessionConfig.load()

        guard config.mode == "editing", let label = config.label else {
            throw CLIError.notInEditingMode
        }

        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .editing(label: label))
        let cwd = config.cwd ?? "/"

        switch PathScheme.parse(path, relativeTo: cwd) {
        case .remote(let remotePath):
            try await session.discard(path: remotePath)
            print("Discarded \(remotePath)")
        case .local:
            throw CLIError.invalidPath("discard requires kr: path")
        }
    }
}

// MARK: - Begin Command

struct Begin: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Start transaction"
    )

    func run() async throws {
        let config = SessionConfig.load()

        guard config.mode == "editing", let label = config.label else {
            throw CLIError.notInEditingMode
        }

        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .editing(label: label))
        try await session.beginEditing()

        print("Transaction started")
    }
}

// MARK: - Commit Command

struct Commit: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "End transaction (commit buffered changes)"
    )

    func run() async throws {
        let config = SessionConfig.load()

        guard config.mode == "editing", let label = config.label else {
            throw CLIError.notInEditingMode
        }

        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .editing(label: label))
        try await session.endEditing()

        print("Transaction committed")
    }
}

// MARK: - Rollback Command (Editor)

struct Rollback: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Rollback transaction (discard buffered changes)"
    )

    func run() async throws {
        let config = SessionConfig.load()

        guard config.mode == "editing", let label = config.label else {
            throw CLIError.notInEditingMode
        }

        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .editing(label: label))
        try await session.rollback()

        print("Transaction rolled back")
    }
}

// MARK: - Submit Command

struct Submit: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Submit for review"
    )

    @Argument(help: "Commit message")
    var message: String

    func run() async throws {
        var config = SessionConfig.load()

        guard config.mode == "editing", let label = config.label else {
            throw CLIError.notInEditingMode
        }

        let storage = try await createStorage(from: config)
        let session = try await ContentSession(storage: storage, mode: .editing(label: label))
        try await session.submit(message: message)

        let editionId = await session.editionId

        // Update config to submitted state
        config.mode = "submitted"
        try config.save()

        print("Submitted edition \(editionId): \(message)")
    }
}
