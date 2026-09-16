param(
    [string]$Addr = "127.0.0.1:9101",
    [Parameter(Mandatory = $true)][string]$Key
)

$exe = "$PSScriptRoot\..\raftctl.exe"
$ctlArgs = @("-addr=$Addr", "delete", $Key)
& $exe @ctlArgs
