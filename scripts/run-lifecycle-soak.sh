#!/usr/bin/env bash
#
# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
#
# run-lifecycle-soak.sh — deliberately UN-isolated lifecycle/churn qualification
# harness for the LoLa transport.
#
# Motivation: the R2V matrix runs each row in a fresh namespace, which is the
# right way to evidence ROUTE correctness — and the wrong way to evidence the
# transport's LIFECYCLE envelope. This harness cycles a workload many times on
# the shared host state ON PURPOSE (shared /tmp/mw_com_lola, shared abstract
# message-passing namespace), across teardown modes, and captures enough
# forensics to identify failures at the abort site.
#
# Known seed signal (R2V clean runs, pristine S-CORE, full isolation): ~2% of
# cold starts SIGABRT in a role process BEFORE its first log line. This harness
# exists to reproduce that with core dumps enabled and to qualify a restart-rate
# envelope for production programs.
#
# Usage:
#   scripts/run-lifecycle-soak.sh --cycles 200 --mode sigint  -- <cmd> [args...]
#   scripts/run-lifecycle-soak.sh --cycles 200 --mode sigkill --grace 0 -- <cmd>
#   scripts/run-lifecycle-soak.sh --cycles 500 --mode alternate --rate 700 -- <cmd>
#
#   <cmd> is one full workload cycle (e.g. a client/server pair driver such as
#   the streamer role-binary payload proof, or a bazel-built example). The
#   harness runs it repeatedly; teardown mode controls how the workload is
#   stopped when --ttl is set, otherwise the workload's own exit is the cycle.
#
# Options:
#   --cycles N     number of cycles (default 100)
#   --mode M       sigint | sigkill | alternate (default sigint)
#   --ttl SECS     kill the workload after SECS (default: wait for exit)
#   --grace SECS   SIGINT->SIGKILL grace in sigint mode (default 5)
#   --rate PER_HR  target cycle starts per hour; sleeps to pace (default: none)
#   --out DIR      artifact root (default target/lifecycle-soak/<timestamp>)
#   --keep N       keep artifacts for at most N failed cycles (default 20)
#
# Per failed cycle the harness records: exit status/signal, full stdout+stderr,
# a core dump (cores are enabled via ulimit and kernel.core_pattern is
# recorded; if cores land elsewhere — e.g. systemd-coredump — the journal id is
# recorded instead), and a gdb backtrace when gdb + a core are available.
# The summary line is machine-parseable:
#   SOAK RESULT cycles=<n> ok=<n> failed=<n> sigabrt=<n> first_fail_cycle=<n|->
set -eu

CYCLES=100 MODE=sigint TTL="" GRACE=5 RATE="" OUT="" KEEP=20
while [ $# -gt 0 ]; do
  case "$1" in
    --cycles) CYCLES="$2"; shift 2 ;;
    --mode)   MODE="$2";   shift 2 ;;
    --ttl)    TTL="$2";    shift 2 ;;
    --grace)  GRACE="$2";  shift 2 ;;
    --rate)   RATE="$2";   shift 2 ;;
    --out)    OUT="$2";    shift 2 ;;
    --keep)   KEEP="$2";   shift 2 ;;
    --) shift; break ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[ $# -gt 0 ] || { echo "missing workload command after --" >&2; exit 2; }

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT:-target/lifecycle-soak/$STAMP}"
mkdir -p "$OUT"

# Enable core dumps for children; record where the kernel will put them.
ulimit -c unlimited || echo "warning: unable to raise core limit" >&2
CORE_PATTERN="$(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo unknown)"
{
  echo "generated: $STAMP"
  echo "mode: $MODE ttl: ${TTL:-none} grace: $GRACE rate: ${RATE:-unpaced}/hr"
  echo "core_pattern: $CORE_PATTERN"
  echo "workload: $*"
  echo "host_state: shared (deliberately un-isolated)"
} > "$OUT/soak-manifest.txt"

SLEEP=""
if [ -n "$RATE" ]; then
  # seconds per cycle to hit the target rate; integer floor is fine for pacing.
  SLEEP=$(( 3600 / RATE ))
fi

ok=0 failed=0 sigabrt=0 first_fail="-"
kept=0
for i in $(seq 1 "$CYCLES"); do
  cycle_mode="$MODE"
  if [ "$MODE" = alternate ]; then
    if [ $(( i % 2 )) -eq 0 ]; then cycle_mode=sigkill; else cycle_mode=sigint; fi
  fi
  log="$OUT/cycle-$i.log"
  start_dir="$PWD"
  ( exec "$@" ) > "$log" 2>&1 &
  pid=$!

  status=0
  if [ -n "$TTL" ]; then
    sleep "$TTL" || true
    if kill -0 "$pid" 2>/dev/null; then
      if [ "$cycle_mode" = sigint ]; then
        kill -INT "$pid" 2>/dev/null || true
        for _ in $(seq 1 "$GRACE"); do
          kill -0 "$pid" 2>/dev/null || break
          sleep 1
        done
      fi
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  wait "$pid" || status=$?

  # Deliberate kills are not failures; only unexpected statuses count.
  expected=0
  if [ -n "$TTL" ]; then
    case "$status" in 0|130|137|143) expected=1 ;; esac
  else
    [ "$status" -eq 0 ] && expected=1
  fi

  if [ "$expected" -eq 1 ]; then
    ok=$(( ok + 1 ))
    rm -f "$log"
  else
    failed=$(( failed + 1 ))
    [ "$first_fail" = "-" ] && first_fail=$i
    sig=$(( status - 128 ))
    [ "$sig" -eq 6 ] && sigabrt=$(( sigabrt + 1 ))
    if [ "$kept" -lt "$KEEP" ]; then
      kept=$(( kept + 1 ))
      fail_dir="$OUT/failed-cycle-$i"
      mkdir -p "$fail_dir"
      mv "$log" "$fail_dir/output.log"
      echo "status=$status signal=$sig mode=$cycle_mode" > "$fail_dir/status.txt"
      # Collect a core if one landed in CWD (classic core_pattern) …
      for core in "$start_dir"/core "$start_dir"/core.*; do
        [ -e "$core" ] || continue
        mv "$core" "$fail_dir/" || true
      done
      # … or record the systemd-coredump reference if that's the pattern.
      case "$CORE_PATTERN" in
        *systemd-coredump*)
          coredumpctl list --no-legend --since=-2min > "$fail_dir/coredumpctl.txt" 2>/dev/null || true ;;
      esac
      # Backtrace when possible.
      core_file=$(ls "$fail_dir"/core* 2>/dev/null | head -1 || true)
      if [ -n "${core_file:-}" ] && command -v gdb >/dev/null 2>&1; then
        gdb -batch -ex bt -ex 'thread apply all bt' "$1" "$core_file" \
          > "$fail_dir/backtrace.txt" 2>&1 || true
      fi
    else
      rm -f "$log"
    fi
  fi

  [ -n "$SLEEP" ] && sleep "$SLEEP"
done

echo "SOAK RESULT cycles=$CYCLES ok=$ok failed=$failed sigabrt=$sigabrt first_fail_cycle=$first_fail" \
  | tee -a "$OUT/soak-manifest.txt"
[ "$failed" -eq 0 ]
