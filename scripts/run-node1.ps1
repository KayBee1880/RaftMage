$exe = "$PSScriptRoot\..\raftmaged.exe"
$nodeArgs = @(
    "-id=node-1",
    "-raft-addr=127.0.0.1:9001",
    "-client-addr=127.0.0.1:9101",
    "-peers=node-2=127.0.0.1:9002,node-3=127.0.0.1:9003"
)
& $exe @nodeArgs
