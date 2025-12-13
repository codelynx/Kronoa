//
//  DevStorageServer.swift
//  Kronoa
//
//  Lightweight HTTP server exposing StorageBackend over HTTP.
//  Used for development testing - allows consumer apps to access
//  producer's local storage over the network.
//  DEBUG builds only.
//

#if DEBUG

import Combine
import Foundation
import Network

// MARK: - DevStorageServer

/// HTTP server that exposes StorageBackend over HTTP for development testing.
@MainActor
public class DevStorageServer: ObservableObject {
	private let storage: StorageBackend
	private var listener: NWListener?
	private var connections: [NWConnection] = []

	@Published public private(set) var isRunning = false
	@Published public private(set) var boundURL: URL?
	@Published public private(set) var boundInterface: String?
	@Published public private(set) var accessToken: String?

	/// Whether to require access token for requests (default: false)
	public var requireToken = false

	public init(storage: StorageBackend) {
		self.storage = storage
	}

	deinit {
		self.listener?.cancel()
	}

	// MARK: - Server Control

	/// Start the server on an available port.
	/// - Parameter port: Starting port to try (will fallback if in use)
	/// - Returns: The URL clients can connect to
	@discardableResult
	public func start(port: UInt16 = 8765) async throws -> URL {
		guard !self.isRunning else {
			if let url = self.boundURL { return url }
			throw DevServerError.alreadyRunning
		}

		// Generate access token if required
		if self.requireToken {
			self.accessToken = UUID().uuidString
		}

		// Find available port
		let actualPort = try self.findAvailablePort(starting: port)

		// Find bind address
		guard let bindAddress = self.selectBindAddress() else {
			throw DevServerError.noSuitableInterface
		}

		// Create listener
		// Note: NWListener doesn't support binding to specific interface directly.
		// We bind to all interfaces but advertise only the selected private LAN IP.
		let parameters = NWParameters.tcp
		parameters.allowLocalEndpointReuse = true

		let listener = try NWListener(using: parameters, on: NWEndpoint.Port(rawValue: actualPort)!)
		self.listener = listener

		listener.stateUpdateHandler = { [weak self] state in
			Task { @MainActor in
				switch state {
				case .ready:
					self?.isRunning = true
				case .failed, .cancelled:
					self?.isRunning = false
					self?.boundURL = nil
				default:
					break
				}
			}
		}

		listener.newConnectionHandler = { [weak self] connection in
			Task { @MainActor in
				self?.handleNewConnection(connection)
			}
		}

		listener.start(queue: .main)

		// Build URL
		let url = URL(string: "http://\(bindAddress):\(actualPort)")!
		self.boundURL = url
		self.boundInterface = self.detectInterfaceName(for: bindAddress)
		self.isRunning = true

		return url
	}

	/// Stop the server.
	public func stop() async {
		self.stopSync()
	}

	private func stopSync() {
		self.listener?.cancel()
		self.listener = nil

		for connection in self.connections {
			connection.cancel()
		}
		self.connections.removeAll()

		self.isRunning = false
		self.boundURL = nil
		self.boundInterface = nil
		self.accessToken = nil
	}

	// MARK: - Port Selection

	private func findAvailablePort(starting: UInt16, max: UInt16 = 8775) throws -> UInt16 {
		for port in starting ... max {
			if self.isPortAvailable(port) {
				return port
			}
		}
		throw DevServerError.noAvailablePort
	}

	private func isPortAvailable(_ port: UInt16) -> Bool {
		let socketFD = socket(AF_INET, SOCK_STREAM, 0)
		guard socketFD >= 0 else { return false }
		defer { close(socketFD) }

		var addr = sockaddr_in()
		addr.sin_family = sa_family_t(AF_INET)
		addr.sin_port = port.bigEndian
		addr.sin_addr.s_addr = INADDR_ANY

		let result = withUnsafePointer(to: &addr) {
			$0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
				bind(socketFD, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
			}
		}

		return result == 0
	}

	// MARK: - Interface Selection

	private func selectBindAddress() -> String? {
		let validPrefixes = ["192.168.", "10.", "172.16.", "172.17.", "172.18.", "172.19.",
		                     "172.20.", "172.21.", "172.22.", "172.23.", "172.24.", "172.25.",
		                     "172.26.", "172.27.", "172.28.", "172.29.", "172.30.", "172.31."]

		var ifaddr: UnsafeMutablePointer<ifaddrs>?
		guard getifaddrs(&ifaddr) == 0, let firstAddr = ifaddr else { return nil }
		defer { freeifaddrs(ifaddr) }

		var addresses: [(name: String, ip: String)] = []

		var ptr: UnsafeMutablePointer<ifaddrs>? = firstAddr
		while let addr = ptr {
			defer { ptr = addr.pointee.ifa_next }

			let interface = addr.pointee
			let family = interface.ifa_addr.pointee.sa_family

			guard family == UInt8(AF_INET) else { continue }

			let name = String(cString: interface.ifa_name)
			guard !name.hasPrefix("lo") else { continue }

			var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
			getnameinfo(
				interface.ifa_addr,
				socklen_t(interface.ifa_addr.pointee.sa_len),
				&hostname,
				socklen_t(hostname.count),
				nil,
				0,
				NI_NUMERICHOST
			)

			let ip = String(cString: hostname)
			if validPrefixes.contains(where: { ip.hasPrefix($0) }) {
				addresses.append((name, ip))
			}
		}

		// Prefer en0 (WiFi) or en1
		if let preferred = addresses.first(where: { $0.name == "en0" || $0.name == "en1" }) {
			return preferred.ip
		}

		return addresses.first?.ip
	}

	private func detectInterfaceName(for ip: String) -> String? {
		var ifaddr: UnsafeMutablePointer<ifaddrs>?
		guard getifaddrs(&ifaddr) == 0, let firstAddr = ifaddr else { return nil }
		defer { freeifaddrs(ifaddr) }

		var ptr: UnsafeMutablePointer<ifaddrs>? = firstAddr
		while let addr = ptr {
			defer { ptr = addr.pointee.ifa_next }

			let interface = addr.pointee
			guard interface.ifa_addr.pointee.sa_family == UInt8(AF_INET) else { continue }

			var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
			getnameinfo(
				interface.ifa_addr,
				socklen_t(interface.ifa_addr.pointee.sa_len),
				&hostname,
				socklen_t(hostname.count),
				nil,
				0,
				NI_NUMERICHOST
			)

			if String(cString: hostname) == ip {
				return String(cString: interface.ifa_name)
			}
		}

		return nil
	}

	// MARK: - Connection Handling

	private func handleNewConnection(_ connection: NWConnection) {
		self.connections.append(connection)

		connection.stateUpdateHandler = { [weak self, weak connection] state in
			if case .cancelled = state, let conn = connection {
				Task { @MainActor in
					self?.connections.removeAll { $0 === conn }
				}
			}
		}

		connection.start(queue: .main)
		self.receiveRequest(on: connection)
	}

	private func receiveRequest(on connection: NWConnection) {
		connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, isComplete, error in
			guard let self = self, let data = data, !data.isEmpty else {
				if isComplete || error != nil {
					connection.cancel()
				}
				return
			}

			Task { @MainActor in
				await self.handleHTTPRequest(data: data, connection: connection)
			}
		}
	}

	private func handleHTTPRequest(data: Data, connection: NWConnection) async {
		guard let requestString = String(data: data, encoding: .utf8) else {
			self.sendResponse(connection: connection, status: 400, body: self.errorJSON("invalid_request", "Cannot parse request"))
			return
		}

		// Parse HTTP request
		let lines = requestString.components(separatedBy: "\r\n")
		guard let requestLine = lines.first else {
			self.sendResponse(connection: connection, status: 400, body: self.errorJSON("invalid_request", "Empty request"))
			return
		}

		let parts = requestLine.split(separator: " ")
		guard parts.count >= 2, parts[0] == "GET" else {
			self.sendResponse(connection: connection, status: 405, body: self.errorJSON("method_not_allowed", "Only GET supported"))
			return
		}

		let urlString = String(parts[1])

		// Parse headers
		var headers: [String: String] = [:]
		for line in lines.dropFirst() {
			if line.isEmpty { break }
			if let colonIndex = line.firstIndex(of: ":") {
				let key = String(line[..<colonIndex]).trimmingCharacters(in: .whitespaces).lowercased()
				let value = String(line[line.index(after: colonIndex)...]).trimmingCharacters(in: .whitespaces)
				headers[key] = value
			}
		}

		// Check access token if required
		if self.requireToken, let token = self.accessToken {
			let authHeader = headers["authorization"] ?? ""
			let expectedAuth = "Bearer \(token)"
			if authHeader != expectedAuth {
				self.sendResponse(connection: connection, status: 401, body: self.errorJSON("unauthorized", "Invalid or missing access token"))
				return
			}
		}

		// Route request
		await self.routeRequest(urlString: urlString, connection: connection)
	}

	private func routeRequest(urlString: String, connection: NWConnection) async {
		guard let components = URLComponents(string: urlString) else {
			self.sendResponse(connection: connection, status: 400, body: self.errorJSON("invalid_url", "Cannot parse URL"))
			return
		}

		let path = components.path
		let queryItems = components.queryItems ?? []

		switch path {
		case "/health":
			let json = #"{"status":"ok","storage":"local"}"#
			self.sendResponse(connection: connection, status: 200, body: Data(json.utf8), contentType: "application/json")

		case "/storage/read":
			guard let filePath = queryItems.first(where: { $0.name == "path" })?.value else {
				self.sendResponse(connection: connection, status: 400, body: self.errorJSON("missing_path", "path parameter required"))
				return
			}
			await self.handleRead(path: filePath, connection: connection)

		case "/storage/exists":
			guard let filePath = queryItems.first(where: { $0.name == "path" })?.value else {
				self.sendResponse(connection: connection, status: 400, body: self.errorJSON("missing_path", "path parameter required"))
				return
			}
			await self.handleExists(path: filePath, connection: connection)

		case "/storage/list":
			let prefix = queryItems.first(where: { $0.name == "prefix" })?.value ?? ""
			let delimiter = queryItems.first(where: { $0.name == "delimiter" })?.value
			await self.handleList(prefix: prefix, delimiter: delimiter, connection: connection)

		default:
			self.sendResponse(connection: connection, status: 404, body: self.errorJSON("not_found", "Unknown endpoint: \(path)"))
		}
	}

	// MARK: - Request Handlers

	private func handleRead(path: String, connection: NWConnection) async {
		guard let validatedPath = self.validatePath(path) else {
			self.sendResponse(connection: connection, status: 400, body: self.errorJSON("invalid_path", "Path validation failed: \(path)"))
			return
		}

		do {
			// For edition content paths, dereference the path file
			let data = try await self.readWithDereference(path: validatedPath)
			let contentType = self.mimeType(for: validatedPath)

			var extraHeaders = "Content-Type: \(contentType)\r\n"
			extraHeaders += "Content-Length: \(data.count)\r\n"

			// Weak ETag based on size
			let etag = "W/\"\(data.count)\""
			extraHeaders += "ETag: \(etag)\r\n"

			self.sendResponse(connection: connection, status: 200, body: data, extraHeaders: extraHeaders)
		} catch {
			self.sendResponse(connection: connection, status: 404, body: self.errorJSON("not_found", "File not found: \(validatedPath)"))
		}
	}

	/// Read content with dereferencing for edition paths.
	/// Edition content uses path files containing "sha256:<hash>" that point to objects.
	private func readWithDereference(path: String) async throws -> Data {
		let rawData = try await self.storage.read(path: path)

		// Check if this is an edition content path (not .catalog or metadata)
		guard path.hasPrefix("contents/editions/") else {
			return rawData
		}

		// Skip metadata files (.catalog, .production.json, etc)
		if path.contains("/.catalog/") || path.hasSuffix(".production.json") || path.hasSuffix(".staging.json") {
			return rawData
		}

		// Try to parse as path file (sha256:<hash>)
		guard rawData.count < 128,
			  let text = String(data: rawData, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines),
			  text.hasPrefix("sha256:") else {
			// Not a path file, return as-is
			return rawData
		}

		// Extract hash and read from object store (contents/objects/<prefix>/<hash>.dat)
		let hash = String(text.dropFirst("sha256:".count))
		let prefix = String(hash.prefix(2))
		let objectPath = "contents/objects/\(prefix)/\(hash).dat"
		return try await self.storage.read(path: objectPath)
	}

	private func handleExists(path: String, connection: NWConnection) async {
		guard let validatedPath = self.validatePath(path) else {
			self.sendResponse(connection: connection, status: 400, body: self.errorJSON("invalid_path", "Path validation failed"))
			return
		}

		do {
			let exists = try await self.storage.exists(path: validatedPath)
			let json = #"{"exists":\#(exists)}"#
			self.sendResponse(connection: connection, status: 200, body: Data(json.utf8), contentType: "application/json")
		} catch {
			let json = #"{"exists":false}"#
			self.sendResponse(connection: connection, status: 200, body: Data(json.utf8), contentType: "application/json")
		}
	}

	private func handleList(prefix: String, delimiter: String?, connection: NWConnection) async {
		// Validate prefix (allow empty)
		let validatedPrefix: String
		if prefix.isEmpty {
			validatedPrefix = ""
		} else {
			guard let validated = self.validatePath(prefix) else {
				self.sendResponse(connection: connection, status: 400, body: self.errorJSON("invalid_path", "Prefix validation failed: \(prefix)"))
				return
			}
			validatedPrefix = validated
		}

		do {
			let files = try await self.storage.list(prefix: validatedPrefix, delimiter: delimiter)

			// Filter results through path validation
			let filteredFiles = files.filter { self.validatePath($0) != nil }

			let escaped = filteredFiles.map { "\"\($0.replacingOccurrences(of: "\"", with: "\\\""))\"" }
			let json = "{\"files\":[\(escaped.joined(separator: ","))]}"
			self.sendResponse(connection: connection, status: 200, body: Data(json.utf8), contentType: "application/json")
		} catch {
			self.sendResponse(connection: connection, status: 500, body: self.errorJSON("internal", "List failed: \(error.localizedDescription)"))
		}
	}

	// MARK: - Path Validation

	private func validatePath(_ path: String) -> String? {
		guard !path.isEmpty else { return nil }
		guard !path.hasPrefix("/") else { return nil }

		let components = path.split(separator: "/", omittingEmptySubsequences: true)
		guard !components.isEmpty else { return nil }
		guard !components.contains("..") else { return nil }

		// Allow known Kronoa dotfiles and metadata prefixes
		let allowedDotFiles = [".production.json", ".staging.json", ".origin", ".flattened", ".head", ".pending"]
		let allowedDotPrefixes = [".catalog"]  // Metadata directories
		for component in components {
			if component.hasPrefix(".") {
				let componentStr = String(component)
				let isAllowedFile = allowedDotFiles.contains(componentStr)
				let isAllowedPrefix = allowedDotPrefixes.contains(where: { componentStr.hasPrefix($0) })
				guard isAllowedFile || isAllowedPrefix else { return nil }
			}
		}

		return components.joined(separator: "/")
	}

	// MARK: - Response Helpers

	private func sendResponse(connection: NWConnection, status: Int, body: Data, contentType: String = "application/json", extraHeaders: String = "") {
		let statusText = self.httpStatusText(status)
		var response = "HTTP/1.1 \(status) \(statusText)\r\n"
		response += "Connection: close\r\n"

		if extraHeaders.isEmpty {
			response += "Content-Type: \(contentType)\r\n"
			response += "Content-Length: \(body.count)\r\n"
		} else {
			response += extraHeaders
		}

		response += "\r\n"

		var responseData = Data(response.utf8)
		responseData.append(body)

		connection.send(content: responseData, completion: .contentProcessed { _ in
			connection.cancel()
		})
	}

	private func errorJSON(_ code: String, _ message: String) -> Data {
		let escaped = message.replacingOccurrences(of: "\"", with: "\\\"")
		return Data(#"{"error":"\#(code)","message":"\#(escaped)"}"#.utf8)
	}

	private func httpStatusText(_ status: Int) -> String {
		switch status {
		case 200: return "OK"
		case 400: return "Bad Request"
		case 401: return "Unauthorized"
		case 404: return "Not Found"
		case 405: return "Method Not Allowed"
		case 500: return "Internal Server Error"
		default: return "Unknown"
		}
	}

	private func mimeType(for path: String) -> String {
		let ext = (path as NSString).pathExtension.lowercased()
		switch ext {
		case "json": return "application/json"
		case "pdf": return "application/pdf"
		case "jpg", "jpeg": return "image/jpeg"
		case "png": return "image/png"
		case "tiff", "tif": return "image/tiff"
		default: return "application/octet-stream"
		}
	}
}

// MARK: - Errors

public enum DevServerError: LocalizedError {
	case alreadyRunning
	case noAvailablePort
	case noSuitableInterface

	public var errorDescription: String? {
		switch self {
		case .alreadyRunning:
			return "Server is already running"
		case .noAvailablePort:
			return "No available port found (tried 8765-8775)"
		case .noSuitableInterface:
			return "No suitable network interface found"
		}
	}
}

#endif
