# Why there is no live broker run from the build VM (2026-10-25)

The harness, adapters, seed determinism and validation logic in this repo
are tested (see logic-tests.txt, seed-check.txt, adapter-load-check.txt).
A live end-to-end run against a real broker could NOT be completed on the
VM where this repo was built, for an environment reason that has nothing
to do with the code:

**The build VM's platform blocks TCP connections initiated by JVM
processes.** When any Java program on this VM opens a TCP connection, the
platform intercepts it and answers with a plain-text policy message
instead of the peer's bytes. Captured verbatim from a minimal Java socket
client's read buffer:

> "Other TCP connections is turned off for this assistant. To allow it,
> ask the user to open Muse settings -> Permissions -> Direct network
> protocols and switch other_tcp from Deny to Ask."

How this kills Apache Kafka: the broker's internal broker-to-controller
registration channel reads that text as a Kafka frame. Its first four
bytes are interpreted as a frame length (~1.8 billion), the client tries
to allocate a buffer that size in a nearly empty heap (GC log at the
moment of failure: 655,360K total, 41,681K used), and the JVM dies with
`java.lang.OutOfMemoryError: Java heap space` at
`NetworkReceive.readFrom`, followed by FatalExitError, seconds after
startup, every time.

What was tried, all with the identical failure signature:

- Apache Kafka 4.1.2 (KRaft, single node) on Temurin JRE 21.0.13+9,
  Temurin JRE 17.0.21+9, Temurin JDK 17.0.21+9, and the Ubuntu system
  package OpenJDK 21.0.12.1 - same result on all four JVMs.
- Heap sizes 320M / 448M / 640M / 768M; default and 127.0.0.1 listeners;
  standard (9092/9093) and alternate (19092/19093) ports.
- Control tests proving the VM's loopback itself is healthy: Python to
  Python, Node to Python, and nc to Python echo tests across processes
  all pass byte-exact. Only JVM-initiated connections are intercepted.
- The harness itself (Node) connects normally: in attempted-live-run.txt
  it gets a plain ECONNREFUSED because no broker process survives.

Also blocked by the same platform policy: node-rdkafka cannot be
installed here, because node-gyp must download Node headers and the
download is denied (403 policy_denied), and it ships no usable prebuild
for this platform. @confluentinc/kafka-javascript installs fine (it ships
prebuilt binaries) and its adapter is included and load-tested.

To complete the live run: switch the setting named above to Ask (and
approve the broker's connection), start the broker, then run
`npm run bench`. The identical code path is also exercised by the Docker
Compose and Codespaces routes in the README, which run in environments
without this restriction. No results in this repository were fabricated:
sample-output contains only runs that actually happened.
