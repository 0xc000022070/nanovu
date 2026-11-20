{
  description = "nanovu: self-hosted server speaking the Novu SDK wire protocol";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      crane,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      build =
        pkgs:
        let
          craneLib = crane.mkLib pkgs.pkgsStatic;
          src = nixpkgs.lib.fileset.toSource {
            root = ./.;
            fileset = nixpkgs.lib.fileset.unions [
              ./Cargo.toml
              ./Cargo.lock
              ./clippy.toml
              ./crates
            ];
          };
          common = {
            inherit src;
            pname = "nanovu";
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly common;
        in
        {
          package = craneLib.buildPackage (
            common
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p nanovu";
              doCheck = false;
              meta.mainProgram = "nanovu";
            }
          );
          test = craneLib.cargoTest (
            common
            // {
              inherit cargoArtifacts;
              SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
            }
          );
          clippy = craneLib.cargoClippy (
            common
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--workspace --all-targets -- -D warnings";
            }
          );
        };
    in
    {
      packages = forAllSystems (pkgs: {
        default = (build pkgs).package;
      });

      checks = forAllSystems (
        pkgs:
        let
          built = build pkgs;
          system = pkgs.stdenv.hostPlatform.system;
          node = nixpkgs.lib.nixosSystem {
            inherit system;
            modules = [
              self.nixosModules.default
              {
                boot.isContainer = true;
                system.stateVersion = "26.05";
                services.nanovu = {
                  enable = true;
                  credentials.stg-secret-key = "/run/keys/stg";
                  settings.environments.stg = {
                    application_identifier = "app-stg";
                    secret_key_file = "/run/credentials/nanovu.service/stg-secret-key";
                  };
                };
              }
            ];
          };
        in
        {
          inherit (built) package test clippy;
          module = node.config.systemd.units."nanovu.service".unit;
        }
      );

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            rustc
            clippy
            rustfmt
            rust-analyzer
            nodejs
            sqlite
          ];
        };
      });

      nixosModules.default = import ./nix/module.nix self;
    };
}
