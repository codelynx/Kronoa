//
//  HTTPStorageBackend.swift
//  Kronoa
//
//  Read-only StorageBackend implementation over HTTP.
//  Connects to DevStorageServer for development testing.
//  DEBUG builds only.
//

#if DEBUG

import Foundation

/// HTTP-based storage backend for connecting to DevStorageServer.
/// Read-only: write operations throw errors.
public final class HTTPStorageBackend: StorageBackend, Sendable {
	private let baseURL: URL
	private let session: URLSession

	/// Initialize with server URL.
	/// - Parameter baseURL: URL to DevStorageServer (e.g., "http://192.168.1.100:8765")
	public init(baseURL: URL) {
		// Normalize: strip trailing slash
		var urlString = baseURL.absoluteString
		while urlString.hasSuffix("/") {
			urlString.removeLast()
		}
		self.baseURL = URL(string: urlString) ?? baseURL

		let config = URLSessionConfiguration.default
		config.timeoutIntervalForRequest = 30
		config.timeoutIntervalForResource = 120
		self.session = URLSession(configuration: config)
	}

	// MARK: - Read Operations

	public func read(path: String) async throws -> Data {
		try PathValidation.validatePath(path)

		var components = URLComponents(url: self.baseURL.appendingPathComponent("/storage/read"), resolvingAgainstBaseURL: false)!
		components.queryItems = [URLQueryItem(name: "path", value: path)]

		guard let url = components.url else {
			throw StorageError.invalidPath(path)
		}

		let (data, response) = try await self.performRequest(url: url)

		guard let httpResponse = response as? HTTPURLResponse else {
			throw StorageError.ioError("Invalid response for \(url)")
		}

		switch httpResponse.statusCode {
		case 200:
			return data
		case 400:
			throw StorageError.invalidPath(path)
		case 404:
			throw StorageError.notFound(path: path)
		default:
			let message = self.parseErrorMessage(from: data) ?? "HTTP \(httpResponse.statusCode)"
			throw StorageError.ioError("Server error: \(message) at \(url)")
		}
	}

	public func exists(path: String) async throws -> Bool {
		try PathValidation.validatePath(path)

		var components = URLComponents(url: self.baseURL.appendingPathComponent("/storage/exists"), resolvingAgainstBaseURL: false)!
		components.queryItems = [URLQueryItem(name: "path", value: path)]

		guard let url = components.url else {
			throw StorageError.invalidPath(path)
		}

		let (data, response) = try await self.performRequest(url: url)

		guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 else {
			return false
		}

		let result = try JSONDecoder().decode(ExistsResponse.self, from: data)
		return result.exists
	}

	public func list(prefix: String, delimiter: String?) async throws -> [String] {
		try PathValidation.validatePrefix(prefix)

		var components = URLComponents(url: self.baseURL.appendingPathComponent("/storage/list"), resolvingAgainstBaseURL: false)!
		var queryItems = [URLQueryItem(name: "prefix", value: prefix)]
		if let delimiter = delimiter {
			queryItems.append(URLQueryItem(name: "delimiter", value: delimiter))
		}
		components.queryItems = queryItems

		guard let url = components.url else {
			return []
		}

		let (data, response) = try await self.performRequest(url: url)

		guard let httpResponse = response as? HTTPURLResponse else {
			return []
		}

		switch httpResponse.statusCode {
		case 200:
			let result = try JSONDecoder().decode(ListResponse.self, from: data)
			return result.files
		case 400:
			throw StorageError.invalidPath(prefix)
		default:
			return []
		}
	}

	// MARK: - Write Operations (Not Supported)

	public func write(path: String, data: Data) async throws {
		throw StorageError.ioError("HTTPStorageBackend is read-only")
	}

	public func writeIfAbsent(path: String, data: Data) async throws -> Bool {
		throw StorageError.ioError("HTTPStorageBackend is read-only")
	}

	public func delete(path: String) async throws {
		throw StorageError.ioError("HTTPStorageBackend is read-only")
	}

	public func atomicIncrement(path: String, initialValue: Int) async throws -> Int {
		throw StorageError.ioError("HTTPStorageBackend is read-only")
	}

	public func acquireLock(path: String, timeout: TimeInterval, leaseDuration: TimeInterval) async throws -> LockHandle {
		throw StorageError.ioError("HTTPStorageBackend is read-only")
	}

	// MARK: - Health Check

	/// Check if server is reachable and healthy.
	public func checkHealth() async -> Bool {
		let url = self.baseURL.appendingPathComponent("/health")

		do {
			let (data, response) = try await self.performRequest(url: url)
			guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 else {
				return false
			}
			let health = try JSONDecoder().decode(HealthResponse.self, from: data)
			return health.status == "ok"
		} catch {
			return false
		}
	}

	// MARK: - Helpers

	private func performRequest(url: URL) async throws -> (Data, URLResponse) {
		do {
			return try await self.session.data(from: url)
		} catch let error as URLError {
			throw StorageError.ioError("\(error.localizedDescription): \(url) (URLError \(error.code.rawValue))")
		} catch {
			throw StorageError.ioError("\(error.localizedDescription): \(url)")
		}
	}

	private func parseErrorMessage(from data: Data) -> String? {
		struct ErrorResponse: Decodable {
			let error: String
			let message: String
		}
		return (try? JSONDecoder().decode(ErrorResponse.self, from: data))?.message
	}

	// MARK: - Response Types

	private struct ExistsResponse: Decodable {
		let exists: Bool
	}

	private struct ListResponse: Decodable {
		let files: [String]
	}

	private struct HealthResponse: Decodable {
		let status: String
		let storage: String?
	}
}

#endif
