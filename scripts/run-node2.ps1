$exe = "$PSScriptRoot\..\raftmaged.exe"
$nodeArgs = @(
    "-id=node-2",
    "-raft-addr=127.0.0.1:9002",
    "-client-addr=127.0.0.1:9102",
    "-peers=node-1=127.0.0.1:9001,node-3=127.0.0.1:9003"
)
& $exe @nodeArgs
