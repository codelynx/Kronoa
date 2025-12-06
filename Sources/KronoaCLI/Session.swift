import ArgumentParser
import Foundation
import Kronoa

// MARK: - Session Configuration

/// Persistent session state stored in .kronoa/session
struct SessionConfig: Codable {
    var storage: String?       // s3://bucket/prefix or file:///path
    var mode: String?          // production, staging, editing
    var label: String?         // Working label (when editing)
    var edition: Int?          // Current edition ID
    var cwd: String?           // Remote current directory

    static let fileName = ".kronoa/session"

    static func load() -> SessionConfig {
        let path = FileManager.default.currentDirectoryPath + "/" + fileName
        guard let data = FileManager.default.contents(atPath: path),
              let config = try? JSONDecoder().decode(SessionConfig.self, from: data) else {
            return SessionConfig()
        }
        return config
    }

    func save() throws {
        let dir = FileManager.default.currentDirectoryPath + "/.kronoa"
        try FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)

        let path = dir + "/session"
        let data = try JSONEncoder().encode(self)
        try data.write(to: URL(fileURLWithPath: path))
    }

    static func clear() throws {
        let path = FileManager.default.currentDirectoryPath + "/" + fileName
        if FileManager.default.fileExists(atPath: path) {
            try FileManager.default.removeItem(atPath: path)
        }
    }
}

// MARK: - Status Command

struct Status: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Show current session state"
    )

    @Flag(name: .long, help: "Output as JSON")
    var json = false

    func run() async throws {
        let config = SessionConfig.load()

        if json {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            let data = try encoder.encode(config)
            print(String(data: data, encoding: .utf8) ?? "{}")
        } else {
            print("Storage: \(config.storage ?? "(not set)")")
            print("Mode:    \(config.mode ?? "(not set)")")
            if let label = config.label {
                print("Label:   \(label)")
            }
            if let edition = config.edition {
                print("Edition: \(edition)")
            }
            print("Cwd:     \(config.cwd ?? "/")")
        }
    }
}

// MARK: - Done Command

struct Done: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Clear session (end editing)"
    )

    func run() async throws {
        try SessionConfig.clear()
        print("Session cleared.")
    }
}

// MARK: - Config Command

struct Config: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Manage session configuration",
        subcommands: [
            ConfigShow.self,
            ConfigClear.self,
            ConfigSet.self,
        ],
        defaultSubcommand: ConfigShow.self
    )
}

struct ConfigShow: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "show",
        abstract: "Show current configuration"
    )

    @Flag(name: .long, help: "Output as JSON")
    var json = false

    func run() async throws {
        let status = Status(json: json)
        try await status.run()
    }
}

struct ConfigClear: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "clear",
        abstract: "Clear session configuration"
    )

    func run() async throws {
        let done = Done()
        try await done.run()
    }
}

struct ConfigSet: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "set",
        abstract: "Set configuration value"
    )

    @Argument(help: "Key to set (storage, cwd)")
    var key: String

    @Argument(help: "Value to set")
    var value: String

    func run() async throws {
        var config = SessionConfig.load()

        let normalizedValue: String
        switch key.lowercased() {
        case "storage":
            // Validate and normalize storage URL
            try validateStorageUrl(value)
            normalizedValue = normalizeStorageUrl(value)
            config.storage = normalizedValue
        case "cwd":
            normalizedValue = value
            config.cwd = normalizedValue
        default:
            throw ValidationError("Unknown config key: \(key). Valid keys: storage, cwd")
        }

        try config.save()
        print("Set \(key) = \(normalizedValue)")
    }
}
