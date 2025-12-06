import ArgumentParser
import Foundation

// MARK: - Pwd Command

struct Pwd: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Show remote current directory"
    )

    func run() async throws {
        let config = SessionConfig.load()
        print(config.cwd ?? "/")
    }
}

// MARK: - Cd Command

struct Cd: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Set remote current directory"
    )

    @Argument(help: "Remote directory path (kr:path or relative)")
    var path: String

    func run() async throws {
        var config = SessionConfig.load()

        // Parse path
        var targetPath = path
        if targetPath.hasPrefix("kr:") {
            targetPath = String(targetPath.dropFirst(3))
        }

        // Handle relative paths
        let currentCwd = config.cwd ?? "/"
        if targetPath == ".." {
            // Go up one level
            var components = currentCwd.split(separator: "/").map(String.init)
            if !components.isEmpty {
                components.removeLast()
            }
            targetPath = components.isEmpty ? "/" : components.joined(separator: "/") + "/"
        } else if targetPath == "/" {
            targetPath = "/"
        } else if !targetPath.hasPrefix("/") {
            // Relative path
            targetPath = currentCwd + targetPath
        }

        // Ensure trailing slash for directories
        if !targetPath.hasSuffix("/") && targetPath != "/" {
            targetPath += "/"
        }

        // Normalize path
        targetPath = normalizePath(targetPath)

        config.cwd = targetPath
        try config.save()
        print(targetPath)
    }

    private func normalizePath(_ path: String) -> String {
        var components = path.split(separator: "/").map(String.init)
        var normalized: [String] = []

        for component in components {
            if component == ".." {
                if !normalized.isEmpty {
                    normalized.removeLast()
                }
            } else if component != "." && !component.isEmpty {
                normalized.append(component)
            }
        }

        if normalized.isEmpty {
            return "/"
        }
        return normalized.joined(separator: "/") + "/"
    }
}
