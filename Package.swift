// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "Kronoa",
    platforms: [
        .macOS(.v15),
        .iOS(.v18)
    ],
    products: [
        .library(
            name: "Kronoa",
            targets: ["Kronoa"]
        ),
    ],
    dependencies: [
        // Future: Add AWS SDK for S3 support
        // .package(url: "https://github.com/soto-project/soto.git", from: "6.0.0"),
    ],
    targets: [
        .target(
            name: "Kronoa",
            dependencies: [
                // Future: Add S3 storage backend
                // .product(name: "SotoS3", package: "soto"),
            ]
        ),
        .testTarget(
            name: "KronoaTests",
            dependencies: ["Kronoa"]
        ),
    ]
)
