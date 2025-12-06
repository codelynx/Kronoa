import ArgumentParser
import Foundation
import Kronoa

// MARK: - Ls Command

struct Ls: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "List remote files"
    )

    @Argument(help: "Remote path (kr:path or relative)")
    var path: String?

    @Flag(name: .long, help: "Output as JSON")
    var json = false

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)

        // Determine path
        let cwd = config.cwd ?? "/"
        let targetPath: String
        if let path = path {
            switch PathScheme.parse(path, relativeTo: cwd) {
            case .remote(let p):
                targetPath = p
            case .local:
                throw CLIError.invalidPath("ls requires kr: path")
            }
        } else {
            targetPath = cwd
        }

        // Get session mode (rejects submitted mode)
        let mode = try getSessionMode(from: config)

        let session = try await ContentSession(storage: storage, mode: mode)

        // Handle glob patterns
        let files: [String]
        if isGlobPattern(targetPath) {
            // List parent directory and filter
            let dir = (targetPath as NSString).deletingLastPathComponent
            let pattern = (targetPath as NSString).lastPathComponent
            let allFiles = try await session.list(directory: dir.isEmpty ? "/" : dir)
            files = matchGlob(pattern: pattern, against: allFiles)
        } else {
            files = try await session.list(directory: targetPath.isEmpty ? "/" : targetPath)
        }

        if json {
            let encoder = JSONEncoder()
            encoder.outputFormatting = .prettyPrinted
            let data = try encoder.encode(files)
            print(String(data: data, encoding: .utf8) ?? "[]")
        } else {
            for file in files {
                print(file)
            }
        }
    }
}

// MARK: - Cat Command

struct Cat: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Print remote file contents"
    )

    @Argument(help: "Remote path(s) (kr:path)")
    var paths: [String]

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)

        // Get session mode (rejects submitted mode)
        let mode = try getSessionMode(from: config)

        let session = try await ContentSession(storage: storage, mode: mode)
        let cwd = config.cwd ?? "/"

        for path in paths {
            switch PathScheme.parse(path, relativeTo: cwd) {
            case .remote(let remotePath):
                // Handle glob
                let filesToRead: [String]
                if isGlobPattern(remotePath) {
                    let dir = (remotePath as NSString).deletingLastPathComponent
                    let pattern = (remotePath as NSString).lastPathComponent
                    let allFiles = try await session.list(directory: dir.isEmpty ? "/" : dir)
                    filesToRead = matchGlob(pattern: pattern, against: allFiles).map {
                        dir.isEmpty ? $0 : dir + "/" + $0
                    }
                } else {
                    filesToRead = [remotePath]
                }

                for file in filesToRead {
                    let data = try await session.read(path: file)
                    if let content = String(data: data, encoding: .utf8) {
                        print(content, terminator: "")
                    } else {
                        FileHandle.standardOutput.write(data)
                    }
                }

            case .local:
                throw CLIError.invalidPath("cat requires kr: path")
            }
        }
    }
}

// MARK: - Write Command

struct Write: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Write to remote file from stdin"
    )

    @Argument(help: "Remote path (kr:path)")
    var path: String

    @Flag(name: .long, help: "Create empty file")
    var empty = false

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
            let data: Data
            if empty {
                data = Data()
            } else {
                data = FileHandle.standardInput.readDataToEndOfFile()
            }
            try await session.write(path: remotePath, data: data)
            print("Wrote \(data.count) bytes to \(remotePath)")

        case .local:
            throw CLIError.invalidPath("write requires kr: path")
        }
    }
}

// MARK: - Cp Command

struct Cp: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Copy files between local and remote"
    )

    @Argument(help: "Source path (file: or kr:)")
    var source: String

    @Argument(help: "Destination path (file: or kr:)")
    var destination: String

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let cwd = config.cwd ?? "/"

        // Get session mode (rejects submitted mode)
        let mode = try getSessionMode(from: config)

        let session = try await ContentSession(storage: storage, mode: mode)

        let src = PathScheme.parse(source, relativeTo: cwd)
        let dst = PathScheme.parse(destination, relativeTo: cwd)

        switch (src, dst) {
        case (.local(let localPath), .remote(let remotePath)):
            // Upload: file: -> kr:
            guard config.mode == "editing" else {
                throw CLIError.notInEditingMode
            }
            let data = try Data(contentsOf: URL(fileURLWithPath: localPath))
            try await session.write(path: remotePath, data: data)
            print("Uploaded \(localPath) -> \(remotePath)")

        case (.remote(let remotePath), .local(let localPath)):
            // Download: kr: -> file:
            // Handle glob for download
            let filesToDownload: [(remote: String, local: String)]
            if isGlobPattern(remotePath) {
                let dir = (remotePath as NSString).deletingLastPathComponent
                let pattern = (remotePath as NSString).lastPathComponent
                let allFiles = try await session.list(directory: dir.isEmpty ? "/" : dir)
                let matches = matchGlob(pattern: pattern, against: allFiles)
                filesToDownload = matches.map { file in
                    let remote = dir.isEmpty ? file : dir + "/" + file
                    let local = localPath.hasSuffix("/") ? localPath + file : localPath
                    return (remote, local)
                }
            } else {
                filesToDownload = [(remotePath, localPath)]
            }

            for (remote, local) in filesToDownload {
                let data = try await session.read(path: remote)
                try data.write(to: URL(fileURLWithPath: local))
                print("Downloaded \(remote) -> \(local)")
            }

        case (.remote(let srcPath), .remote(let dstPath)):
            // Remote copy: kr: -> kr:
            guard config.mode == "editing" else {
                throw CLIError.notInEditingMode
            }
            try await session.copy(from: srcPath, to: dstPath)
            print("Copied \(srcPath) -> \(dstPath)")

        case (.local, .local):
            throw CLIError.invalidPath("Both paths are local. Use system cp command.")
        }
    }
}

// MARK: - Rm Command

struct Rm: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Delete remote file"
    )

    @Argument(help: "Remote path (kr:path)")
    var path: String

    @Flag(name: .long, help: "Allow glob patterns")
    var glob = false

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
            if isGlobPattern(remotePath) {
                guard glob else {
                    throw CLIError.globRequiresFlag
                }
                // Expand glob and delete each
                let dir = (remotePath as NSString).deletingLastPathComponent
                let pattern = (remotePath as NSString).lastPathComponent
                let allFiles = try await session.list(directory: dir.isEmpty ? "/" : dir)
                let matches = matchGlob(pattern: pattern, against: allFiles)

                print("Will delete \(matches.count) file(s):")
                for file in matches {
                    print("  \(file)")
                }

                for file in matches {
                    let fullPath = dir.isEmpty ? file : dir + "/" + file
                    try await session.delete(path: fullPath)
                }
                print("Deleted \(matches.count) file(s)")
            } else {
                try await session.delete(path: remotePath)
                print("Deleted \(remotePath)")
            }

        case .local:
            throw CLIError.invalidPath("rm requires kr: path")
        }
    }
}

// MARK: - Stat Command

struct Stat: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Show file metadata"
    )

    @Argument(help: "Remote path (kr:path)")
    var path: String

    @Flag(name: .long, help: "Output as JSON")
    var json = false

    func run() async throws {
        let config = SessionConfig.load()
        let storage = try await createStorage(from: config)
        let cwd = config.cwd ?? "/"

        // Get session mode (rejects submitted mode)
        let mode = try getSessionMode(from: config)

        let session = try await ContentSession(storage: storage, mode: mode)

        switch PathScheme.parse(path, relativeTo: cwd) {
        case .remote(let remotePath):
            let stat = try await session.stat(path: remotePath)

            if json {
                let output: [String: Any] = [
                    "path": stat.path,
                    "status": "\(stat.status)",
                    "resolvedFrom": stat.resolvedFrom,
                    "hash": stat.hash as Any,
                    "size": stat.size as Any
                ]
                if let data = try? JSONSerialization.data(withJSONObject: output, options: .prettyPrinted),
                   let str = String(data: data, encoding: .utf8) {
                    print(str)
                }
            } else {
                print("Path:     \(stat.path)")
                print("Status:   \(stat.status)")
                print("Edition:  \(stat.resolvedFrom)")
                if let hash = stat.hash {
                    print("Hash:     \(hash)")
                }
                if let size = stat.size {
                    print("Size:     \(size) bytes")
                }
            }

        case .local:
            throw CLIError.invalidPath("stat requires kr: path")
        }
    }
}
