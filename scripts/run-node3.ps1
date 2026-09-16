$exe = "$PSScriptRoot\..\raftmaged.exe"
$nodeArgs = @(
    "-id=node-3",
    "-raft-addr=127.0.0.1:9003",
    "-client-addr=127.0.0.1:9103",
    "-peers=node-1=127.0.0.1:9001,node-2=127.0.0.1:9002"
)
& $exe @nodeArgs
