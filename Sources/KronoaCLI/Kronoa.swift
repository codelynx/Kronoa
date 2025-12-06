import ArgumentParser
import Foundation

@main
struct KronoaCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "kronoa",
        abstract: "Kronoa content management CLI",
        version: "0.6.0",
        subcommands: [
            // Session
            Status.self,
            Done.self,
            Config.self,

            // Navigation
            Pwd.self,
            Cd.self,

            // File operations
            Ls.self,
            Cat.self,
            Write.self,
            Cp.self,
            Rm.self,
            Stat.self,

            // Editor workflow
            Checkout.self,
            Discard.self,
            Begin.self,
            Commit.self,
            Rollback.self,
            Submit.self,

            // Admin workflow
            Pending.self,
            Stage.self,
            Reject.self,
            Rejected.self,
            Deploy.self,
            AdminRollback.self,

            // Maintenance
            Flatten.self,
            Gc.self,
        ],
        defaultSubcommand: Status.self
    )
}
