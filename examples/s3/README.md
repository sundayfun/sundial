# S3 example

## Quick start

Set the S3 location first. The example uses the AWS SDK default credential
chain:

```sh
export AWS_REGION=us-east-1
export SUNDIAL_S3_BUCKET=my-config-bucket
export SUNDIAL_S3_CURRENT_REVISION_KEY=production/app/metadata.yaml
export SUNDIAL_S3_REVISION_KEY_PREFIX=production/app/

go run ./examples/s3 -init ./examples/s3/config.yaml
```

`-init` publishes the file before loading it. It is only needed when creating
or resetting the example configuration.

Update the port with a conditional write:

```sh
go run ./examples/s3 -port 9090
```

The update is conditional: it fails with a conflict if another writer publishes
a revision after `Update` creates its draft.

The example uses the YAML codec. `metadata.yaml` contains `current_revision_id`;
`<revision-id>.yaml` stores the original business configuration.
Revision IDs use ULID. S3 user metadata uses `parent-id` to link each revision
to its parent.

The example creates its client with `sundial.New` and the YAML codec; callers do
not need to supply a clone function. `Get` returns a shared read-only typed
snapshot without encoding, decoding, deep copying or an error return. Callers
must not modify it; Go does not enforce this contract.

`Update` is the client's only ordinary write interface and needs no preceding
`Get` for editing. Internally, it decodes the cached source document into an
independent draft, applies the callback, then validates and isolates the edited
value through encoding and decoding before publishing conditionally. Each
update uses one encode and two decodes. The snapshot retains the source
document; reparsing it may differ from the parsed snapshot if the codec injects
dynamic values.
Failed edits or writes leave the cached snapshot unchanged. Successful write
results are also shared read-only snapshots.

The update callback must not call `Update`, `Reload` or `RestoreRevision` on the
same client. See the [read and write contract](../../README.md#quick-start) for
codec requirements.
