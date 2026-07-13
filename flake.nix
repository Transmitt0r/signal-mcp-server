{
  description = "Signal MCP Server — signal-cli daemon JSON-RPC API as MCP tools";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        nodejs = pkgs.nodejs_22;

        build = pkgs.stdenv.mkDerivation {
          pname = "signal-mcp-server";
          version = "0.3.0";
          src = ./.;

          nativeBuildInputs = [ pkgs.pnpm nodejs ];

          buildPhase = ''
            export HOME=$TMPDIR
            export PUPPETEER_SKIP_DOWNLOAD=true
            pnpm install --frozen-lockfile --no-optional
            pnpm build
          '';

          installPhase = ''
            mkdir -p $out
            cp -r dist package.json $out/
          '';

          # Tests require a running signal-cli daemon — skip in build derivation
          doCheck = false;
        };

        container = pkgs.dockerTools.buildLayeredImage {
          name = "ghcr.io/transmitt0r/signal-mcp-server";
          tag = "latest";
          contents = [ build nodejs pkgs.busybox ];
          config = {
            Cmd = [ "${nodejs}/bin/node" "${build}/dist/index.js" ];
            Env = [
              "SIGNAL_HTTP_URL=http://127.0.0.1:8080"
              "SIGNAL_MCP_MAX_MSGS=500"
              "PATH=${nodejs}/bin"
            ];
          };
          maxLayers = 60;
        };

        # Pure Nix build using prefetched dependencies (experimental)
        # offline-build = pkgs.stdenv.mkDerivation { ... };
      in
      {
        packages.default = build;
        packages.container = container;
        packages.docker = container;

        apps.default = {
          type = "app";
          program = "${build}/dist/index.js";
        };

        devShells.default = pkgs.mkShell {
          buildInputs = [ pkgs.bun pkgs.pnpm nodejs ];
          shellHook = ''
            echo "🔧 signal-mcp-server dev shell"
            echo "   Bun $(bun --version)  Node $(node --version)  pnpm $(pnpm --version)"
          '';
        };
      });
}