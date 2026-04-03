# Copyright (c) 2021 The Regents of the University of California
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met: redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer;
# redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution;
# neither the name of the copyright holders nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""

This script shows an example of running a CXL type 3 memory expander simulation
using the gem5 library with the Classic memory system. It defaults to simulating
a CXL ASIC Device.
This simulation boots Ubuntu 18.04 using KVM CPU cores (switching from Atomic/KVM).
The simulation then switches to TIMING/O3 CPU core to run the benchmark.

Usage
-----

```
scons build/X86/gem5.opt -j21
./build/X86/gem5.opt configs/example/gem5_library/x86-cxl-type3-with-classic.py \
    --kernel /path/to/vmlinux \
    --disk-image /path/to/parsec.img
```
"""
import argparse
from pathlib import Path
import m5
from m5.util.convert import anyToLatency
from gem5.utils.requires import requires
from gem5.components.boards.x86_board import X86Board
from gem5.components.memory.single_channel import DIMM_DDR5_4400, SingleChannelDDR4_3200
from gem5.components.memory.simple import SingleChannelSimpleMemory
from gem5.components.processors.simple_processor import SimpleProcessor
from gem5.components.processors.simple_switchable_processor import SimpleSwitchableProcessor
from gem5.components.processors.cpu_types import CPUTypes
from gem5.components.cachehierarchies.classic.private_l1_private_l2_shared_l3_cache_hierarchy import (
    PrivateL1PrivateL2SharedL3CacheHierarchy,
)
from gem5.isas import ISA
from gem5.simulate.simulator import Simulator
from gem5.simulate.exit_event import ExitEvent
from gem5.resources.resource import DiskImageResource, KernelResource

# Check ensures the gem5 binary is compiled to X86.
requires(isa_required=ISA.X86)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_KERNEL = REPO_ROOT / "files" / "vmlinux"
DEFAULT_DISK_IMAGE = REPO_ROOT / "files" / "parsec.img"
LEGACY_BENCH_ROOT = "/home/cxl_benchmark"
SIMPLE_CXL_LATENCY = "30ns"
SIMPLE_CXL_LATENCY_VAR = "0ns"
SIMPLE_CXL_BANDWIDTH = "12.8GiB/s"

# Argument Parsing
parser = argparse.ArgumentParser(description='CXL system parameters.')
parser.add_argument('--is_asic', action='store', type=str, nargs='?', 
                    choices=['True', 'False'], default='True', 
                    help='Choose to simulate CXL ASIC Device or FPGA Device.')
test_choices = [
    'lmbench_cxl.sh', 'lmbench_dram.sh', 
    'merci_dram.sh', 'merci_cxl.sh', 'merci_dram+cxl.sh',
    'stream_dram.sh', 'stream_cxl.sh'
]
parser.add_argument('--test_cmd', type=str, choices=test_choices, 
                    default='lmbench_cxl.sh', help='Choose a test to run.')

parser.add_argument('--num_cpus', type=int, default=1, help='Number of CPUs')
parser.add_argument('--cpu_type', type=str, choices=['TIMING', 'O3'], 
                    default='TIMING', help='CPU type')
parser.add_argument('--boot_cpu', type=str, choices=['KVM', 'ATOMIC'],
                    default='KVM',
                    help='Boot CPU type before switching.')
parser.add_argument(
    '--terminal-port',
    type=int,
    default=3456,
    help='Host TCP port exposed by the guest COM1 terminal. Use 0 to keep the default file-backed serial log.',
)
parser.add_argument('--cxl_mem_type', type=str, choices=['Simple', 'DRAM'], 
                    default='DRAM', help='CXL memory type')
parser.add_argument(
    '--cxl-bridge-extra-latency',
    type=str,
    default='0ns',
    help='Extra latency added to the Classic-mode host CXL bridge. Default: 0ns',
)
parser.add_argument('--kernel', type=str,
                    default='',
                    help='Path to kernel image (vmlinux).')
parser.add_argument('--disk-image', type=str,
                    default='',
                    help='Path to disk image.')
parser.add_argument(
    '--workload-cmd',
    type=str,
    default='',
    help='Guest command sequence written to readfile. Mutually exclusive with --workload-file.',
)
parser.add_argument(
    '--workload-file',
    type=str,
    default='',
    help='Optional host file whose contents are written to readfile. Mutually exclusive with --workload-cmd and overrides legacy --test_cmd fallback.',
)
parser.add_argument(
    '--readfile-path',
    type=str,
    default='',
    help='Optional host readfile path used directly by the guest via m5 readfile. Mutually exclusive with --workload-file and --workload-cmd.',
)
parser.add_argument(
    '--checkpoint-boot',
    action='store_true',
    help='Boot with the fast boot CPU, execute a checkpoint-prep runscript, and exit after checkpoint generation.',
)
parser.add_argument(
    '--restore-checkpoint',
    type=str,
    default='',
    help='Restore from a checkpoint directory and start directly with --cpu_type cores.',
)
parser.add_argument(
    '--disable-workload',
    action='store_true',
    help='Boot or restore without auto-running any readfile workload.',
)

args = parser.parse_args()

if args.workload_file and args.workload_cmd:
    parser.error("--workload-file and --workload-cmd are mutually exclusive")
if args.readfile_path and (args.workload_file or args.workload_cmd):
    parser.error("--readfile-path is mutually exclusive with --workload-file and --workload-cmd")
if args.checkpoint_boot and args.restore_checkpoint:
    parser.error("--checkpoint-boot and --restore-checkpoint are mutually exclusive")
if args.disable_workload and (
    args.workload_file
    or args.workload_cmd
    or args.checkpoint_boot
):
    parser.error(
        "--disable-workload is mutually exclusive with "
        "--workload-file/--workload-cmd/--checkpoint-boot"
    )

kernel_path = Path(args.kernel) if args.kernel else DEFAULT_KERNEL
disk_image_path = Path(args.disk_image) if args.disk_image else DEFAULT_DISK_IMAGE
restore_checkpoint = Path(args.restore_checkpoint) if args.restore_checkpoint else None
checkpoint_boot_script = REPO_ROOT / "configs" / "boot" / "hack_back_ckpt.rcS"
readfile_path = Path(args.readfile_path) if args.readfile_path else None

if not kernel_path.exists():
    parser.error(
        f"kernel image not found: {kernel_path}. Pass --kernel explicitly."
    )
if not disk_image_path.exists():
    parser.error(
        "disk image not found: "
        f"{disk_image_path}. Pass --disk-image explicitly."
    )
if restore_checkpoint is not None and not restore_checkpoint.exists():
    parser.error(f"checkpoint directory not found: {restore_checkpoint}")
if args.checkpoint_boot and not checkpoint_boot_script.exists():
    parser.error(f"checkpoint boot script not found: {checkpoint_boot_script}")
if readfile_path is not None and not readfile_path.exists():
    parser.error(f"readfile path not found: {readfile_path}")


def add_latency(base_latency: str, extra_latency: str) -> str:
    total_seconds = anyToLatency(base_latency) + anyToLatency(extra_latency)
    return f"{total_seconds * 1e9:.12g}ns"

# Setup Classic MESI Three Level Cache Hierarchy
cache_hierarchy = PrivateL1PrivateL2SharedL3CacheHierarchy(
    l1d_size="48kB",
    l1d_assoc=6,
    l1i_size="32kB",
    l1i_assoc=8,
    l2_size="2MB",
    l2_assoc=16,
    l3_size="96MB",
    l3_assoc=48,
)

# Setup system memory and CXL memory
memory = DIMM_DDR5_4400(size="3GB")
if args.cxl_mem_type == 'Simple':
    cxl_dram = SingleChannelSimpleMemory(
        latency=SIMPLE_CXL_LATENCY,
        latency_var=SIMPLE_CXL_LATENCY_VAR,
        bandwidth=SIMPLE_CXL_BANDWIDTH,
        size="8GB",
    )
else:
    cxl_dram = DIMM_DDR5_4400(size="8GB")
    if args.is_asic == 'False':
        cxl_dram = SingleChannelDDR4_3200(size="8GB")

# Setup Processor
timing_cpu_type = CPUTypes.O3 if args.cpu_type == 'O3' else CPUTypes.TIMING
boot_cpu_type = CPUTypes.KVM if args.boot_cpu == 'KVM' else CPUTypes.ATOMIC

if restore_checkpoint is not None:
    processor = SimpleProcessor(
        cpu_type=timing_cpu_type,
        isa=ISA.X86,
        num_cores=args.num_cpus,
    )
elif args.checkpoint_boot:
    processor = SimpleProcessor(
        cpu_type=boot_cpu_type,
        isa=ISA.X86,
        num_cores=args.num_cpus,
    )
else:
    processor = SimpleSwitchableProcessor(
        starting_core_type=boot_cpu_type,
        switch_core_type=timing_cpu_type,
        isa=ISA.X86,
        num_cores=args.num_cpus,
    )

# Only KVM cores expose `usePerf`.
for core in processor.get_cores():
    if core.is_kvm_core():
        core.core.usePerf = False

# Here we setup the board and CXL device memory size. The X86Board allows for Full-System X86 simulations.
board = X86Board(
    clk_freq="2.4GHz",
    processor=processor,
    memory=memory,
    cache_hierarchy=cache_hierarchy,
    cxl_memory=cxl_dram,
    is_asic=(args.is_asic == 'True'),
    cxl_bridge_latency=add_latency("50ns", args.cxl_bridge_extra_latency),
)
board.pc.com_1.device.port = args.terminal_port

# Here we set the Full System workload.
# The `set_kernel_disk_workload` function for the X86Board takes a kernel, a
# disk image, and, optionally, a command to run.

# This is the command to run after the system has booted. The first `m5 exit`
# will stop the simulation so we can switch the CPU cores from KVM/ATOMIC to 
# TIMING/O3 and continue the simulation to run the command. After simulation
# has ended you may inspect `m5out/board.pc.com_1.device` to see the echo
# output.
command = None
if args.disable_workload:
    command = None
elif readfile_path is not None:
    command = None
elif args.checkpoint_boot:
    command = checkpoint_boot_script.read_text(encoding="utf-8")
elif args.workload_file:
    workload_path = Path(args.workload_file)
    if not workload_path.exists():
        parser.error(
            f"workload file not found: {workload_path}. Pass a valid --workload-file."
        )
    if not workload_path.is_file():
        parser.error(f"workload file is not a regular file: {workload_path}")
    try:
        command = workload_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        parser.error(f"failed to read workload file {workload_path}: {exc}")
elif args.workload_cmd:
    command = args.workload_cmd
else:
    command = (
        "m5 exit;"
        + "numactl -H;"
        + "m5 resetstats;"
        + f"bash {LEGACY_BENCH_ROOT}/{args.test_cmd};"
        + "m5 exit"
    )

# Prefer repo-local files/ by default, or override them explicitly via CLI.
board.set_kernel_disk_workload(
    kernel=KernelResource(local_path=str(kernel_path)),
    disk_image=DiskImageResource(local_path=str(disk_image_path)),
    readfile=str(readfile_path) if readfile_path is not None else None,
    readfile_contents=command,
    checkpoint=restore_checkpoint,
    # Avoid mwait-related guest idle crashes after switching off boot cores.
    kernel_args=board.get_default_kernel_args() + ["idle=poll"],
)


def checkpoint_then_exit_generator():
    checkpoint_dir = Path(m5.options.outdir)
    while True:
        m5.checkpoint((checkpoint_dir / f"cpt.{m5.curTick()}").as_posix())
        yield True


def switch_then_exit_generator():
    switched = False
    while True:
        if not switched:
            processor.switch()
            switched = True
            yield False
        else:
            yield True

if restore_checkpoint is not None or args.checkpoint_boot:
    simulator = Simulator(
        board=board,
        on_exit_event={
            ExitEvent.CHECKPOINT: checkpoint_then_exit_generator(),
        },
    )
else:
    simulator = Simulator(
        board=board,
        on_exit_event={
            ExitEvent.EXIT: switch_then_exit_generator(),
            ExitEvent.CHECKPOINT: checkpoint_then_exit_generator(),
        },
    )

print("Running the simulation Classic MESI Three Level protocol...")
if restore_checkpoint is not None:
    print(
        "Restoring from checkpoint "
        f"{restore_checkpoint} with {args.cpu_type} cores"
    )
elif args.checkpoint_boot:
    print(f"Using {args.boot_cpu} cpu for boot/checkpoint generation")
else:
    print(f"Using {args.boot_cpu} cpu for boot")

m5.stats.reset()

simulator.run()
