case "$TARGETARCH" in
  amd64) DOTNET_RID="linux-x64" ;;
  arm64) DOTNET_RID="linux-arm64" ;;
  arm)   DOTNET_RID="linux-arm" ;;
  *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;;
esac
