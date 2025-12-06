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
        .executable(
            name: "kronoa",
            targets: ["KronoaCLI"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/awslabs/aws-sdk-swift", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-argument-parser", from: "1.5.0"),
    ],
    targets: [
        .target(
            name: "Kronoa",
            dependencies: [
                .product(name: "AWSS3", package: "aws-sdk-swift"),
            ]
        ),
        .executableTarget(
            name: "KronoaCLI",
            dependencies: [
                "Kronoa",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ]
        ),
        .testTarget(
            name: "KronoaTests",
            dependencies: ["Kronoa"]
        ),
    ]
)
