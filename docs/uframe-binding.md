# LoLa UFrame Binding

Binding id: `lola.uframe.ulol.v1`

## Physical Placement

Selected-wire UFrame metadata is carried inside the LoLa event sample after the fixed `ULOL` frame header and before the application payload. The metadata bytes are the UFrame metadata envelope (see up-spec `basics/uframe.adoc`, Metadata envelope and identity registry): magic/version, selected-wire identity, payload-family identity, metadata-layout identity, and the selected metadata profile bytes.

The application payload starts at the aligned payload offset recorded in the `ULOL` header. The `ULOL` header, metadata prefix, and alignment padding are not part of the application payload exposed by selected-wire receive APIs.

## Metadata Profiles

The default profile is the canonical UFrame field-block metadata profile identified by `org.eclipse.uprotocol.metadata.uframe-fields`.

The legacy protobuf-`UAttributes` metadata profile remains compatibility-only and must be selected explicitly by a legacy-named API. Mixed-profile decode is rejected as an unknown metadata layout before a frame is exposed to users.

## Response Channel Mapping

When a response LoLa event channel is configured, RPC response frames are sent on the response channel and other frames use the primary channel. Receive-channel selection uses source and sink filters before metadata decode:

| Filter shape | LoLa receive channel |
| --- | --- |
| Sink is an RPC method | Primary |
| Exact source is an RPC method | Response |
| Any other sink is present | Response |
| Broad source with no sink | Configured default: primary, response, or both |

After a sample is received and decoded, semantic UFrame source and sink filters are applied to the decoded metadata before public frame exposure.

## Manifest And Runtime Constraints

Native LoLa is deployment-manifest driven. `LolaTransportConfig::mw_com_config_path` points to the S-CORE `mw_com_config.json` manifest that defines LoLa service IDs, instance IDs, events, sample slots, and subscriber limits.

The S-CORE runtime is initialized once per process. All native LoLa transports and subscribers in a process must use the same resolved manifest path, or omit the path and use S-CORE's default `./etc/mw_com_config.json`. The Rust and native bridge layers reject a second different manifest path in the same process.

On Linux, S-CORE LoLa stores service-discovery and partial-restart runtime state under `/tmp/mw_com_lola`. That directory is runtime state, not uProtocol configuration. Do not remove it while a LoLa process is running, and this phase does not delete it.

## Malformed Input

Missing, malformed, wrong-wire, wrong-payload-family, wrong-profile, and filter-mismatched `ULOL` samples are rejected before public selected-wire frame exposure. Listener paths drop rejected frames instead of dispatching them.

## Reverse RPC Lifecycle Caveat

The optional response event channel preserves the existing reverse-RPC lifecycle caveat: deployments must keep the response-channel service/event available for the duration of outstanding RPC calls, and partial restarts can leave S-CORE runtime state that must be handled operationally outside uProtocol route configuration.
