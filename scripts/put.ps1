param(
    [string]$Addr = "127.0.0.1:9101",
    [Parameter(Mandatory = $true)][string]$Key,
    [Parameter(Mandatory = $true)][string]$Value
)

$exe = "$PSScriptRoot\..\raftctl.exe"
$ctlArgs = @("-addr=$Addr", "put", $Key, $Value)
& $exe @ctlArgs
