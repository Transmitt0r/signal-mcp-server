{
  description = "Signal MCP Server — development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        nodejs = pkgs.nodejs_22;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [ pkgs.bun pkgs.pnpm nodejs ];
          shellHook = ''
            echo "🔧 signal-mcp-server dev shell"
            echo "   Bun $(bun --version)  Node $(node --version)  pnpm $(pnpm --version)"
          '';
        };
      });
}