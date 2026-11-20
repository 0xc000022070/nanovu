self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.nanovu;
  toml = pkgs.formats.toml { };
in
{
  options.services.nanovu = {
    enable = lib.mkEnableOption "nanovu notification server";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "nanovu.packages.\${system}.default";
    };

    settings = lib.mkOption {
      inherit (toml) type;
      default = { };
      description = ''
        nanovu TOML config. Point `*_file` keys at `/run/credentials/nanovu.service/<name>`
        and list the sources in `credentials`.
      '';
    };

    credentials = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      example = {
        stg-secret-key = "/run/secrets/nanovu-stg-key";
      };
      description = ''
        Absolute paths of secret files outside the Nix store, passed through systemd
        `LoadCredential` by credential name.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = lib.mapAttrsToList (name: path: {
      assertion = lib.hasPrefix "/" path && !lib.hasPrefix builtins.storeDir path;
      message = "services.nanovu.credentials.${name} must be an absolute path outside the Nix store";
    }) cfg.credentials;

    services.nanovu.settings = {
      database = lib.mkDefault "/var/lib/nanovu/nanovu.db";
    };

    systemd.services.nanovu = {
      description = "nanovu notification server";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      environment.SSL_CERT_FILE = "/etc/ssl/certs/ca-certificates.crt";
      serviceConfig = {
        ExecStart = "${lib.getExe cfg.package} serve --config ${toml.generate "nanovu.toml" cfg.settings}";
        Restart = "on-failure";
        DynamicUser = true;
        StateDirectory = "nanovu";
        StateDirectoryMode = "0700";
        UMask = "0077";
        LoadCredential = lib.mapAttrsToList (name: path: "${name}:${path}") cfg.credentials;

        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        NoNewPrivileges = true;
        MemoryDenyWriteExecute = true;
        LockPersonality = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        RestrictNamespaces = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectKernelLogs = true;
        ProtectControlGroups = true;
        ProtectClock = true;
        ProtectHostname = true;
        SystemCallArchitectures = "native";
        SystemCallFilter = [
          "@system-service"
          "~@privileged"
        ];
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        CapabilityBoundingSet = "";
        AmbientCapabilities = "";
        MemoryHigh = "18M";
        MemoryMax = "24M";
      };
    };
  };
}
