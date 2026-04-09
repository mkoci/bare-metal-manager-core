# Mini-Rack Deployment — Static IP + Zero-DPU Bypass

> Author: [mkoci](mailto:mkoci@nvidia.com)
> Based on: [`mrgalaxy/static-ip`](https://github.com/mrgalaxy-source/bare-metal-manager-core/tree/mrgalaxy/static-ip) branch ([PR #757](https://github.com/NVIDIA/ncx-infra-controller-core/pull/757)) by [mgalaxy](mailto:mrgalaxy@nvidia.com)'s
> Zero-DPU approach informed by [Vinod](mailto:vchitrali@nvidia.com)'s and [Syd](mailto:sydneyl)'s 72x1 deployment
> Site controller: `forge-lite-controller` at 10.86.160.103

## Background

We needed a carbide deployment on a mini-rack for HaaS development — real
hardware, not machinetron mocks. The problem is we don't have the full rack
networking stack: no DHCP relay, no external IPs from IT, no PXE boot
infrastructure, and no nautobot. This puts us somewhere between local-dev and
a 72x1 rack deployment.

**Key insight from [Vinod](mailto:vchitrali@nvidia.com):** Use
[mgalaxy](mailto:mrgalaxy@nvidia.com)'s static IP branch
([PR #757](https://github.com/NVIDIA/ncx-infra-controller-core/pull/757)) to
register BMCs by IP address directly, bypassing DHCP discovery entirely. This
is a WIP feature but fits the use case exactly.

What we discovered along the way is that the static IP branch alone isn't
enough. The site explorer still refuses to create machines if it sees DPUs in
the Redfish data but can't find them via DHCP. We also don't run forge-scout on
the compute hosts, so the state machine gets stuck waiting for callbacks. This
guide covers the workarounds for both.

The Confluence guides for
[local-dev](https://nvidia.atlassian.net/wiki/spaces/DCSS/pages/3035466043) and
[72x1 Bringup](https://nvidia.atlassian.net/wiki/spaces/DCSS/pages/3139933046)
cover those respective deployments. This guide covers the mini-rack gap.

---

## What You Need

- Dedicated bare-metal site controller — Ubuntu 24.04, x86_64, 32+ cores,
  128GB+ RAM, 1TB+ NVMe
- GitLab SSH key (for `environments` and `forged` repos)
- NGC API token (`dcim` org — ask [Vinod](mailto:vchitrali@nvidia.com)
  or [Joe](mailto:jshifflett@nvidia.com) if you don't have one)
- AGE secret key (get from a colleague or the AGE Key Creation Guide)
- Docker group: `sudo groupadd docker && sudo usermod -aG docker $USER && newgrp docker`
- BMC IPs, MACs, serials, and credentials for your compute trays and switches
- Network path from site controller to all BMC IPs (verify with ping)

---

## 1. Bootstrap the Cluster

### Clone the [environments](https://gitlab-master.nvidia.com/swserver/RackManagementService/environments) repo

```bash
cd ~
git clone ssh://git@gitlab-master.nvidia.com:12051/swserver/RackManagementService/environments
```

### Create your env file

This is the mini-rack equivalent of `my_localdev_env.sh` from the Confluence
guide, but configured for rack mode with the static IP branch:

```bash
# ~/environments/scripts/setup/mini_devenv.sh

export GITHUB_USER=mrgalaxy-source
export GITHUB_REPO_NAME=bare-metal-manager-core
export CARBIDE_BRANCH=mrgalaxy/static-ip

export AGE_SECRET_KEY=<your key>
export NGC_API_TOKEN=<your token>
export NVCR_AUTH_TOKEN=$(echo -n "\$oauthtoken:$NGC_API_TOKEN" | base64 -w0)
export ROOT_EXEC_CMD=sudo
export CARGO_HOME=$HOME/.cargo

# rack mode — do NOT set LOCAL_DEV
export NO_TEST=1
```

Important differences from the Confluence local-dev guide:
- We clone from `mrgalaxy-source` (the static IP fork), not your own fork
- We use `mrgalaxy/static-ip` branch, not `main`
- `LOCAL_DEV` is **not set** — this is rack mode
- `NO_TEST=1` because the tests will fail in this config

### Run first_run.sh

```bash
cd ~/environments/scripts/setup
source mini_devenv.sh
bash first_run.sh    # ~55 min. use bash -x for debug output
```

This does everything: installs k3s, deploys argocd/vault/postgres/monitoring,
clones carbide from the static IP fork, builds from source, and deploys the
full forge stack.

**Do NOT run `post_run.sh`** — that's for local-dev only (it runs `skaffold dev`).

### Verify

```bash
kubectl get pods -n forge-system
```

You should see `carbide-api`, `carbide-dhcp`, `carbide-dns`, `carbide-pxe`,
`carbide-hardware-health`, etc. Set up a CLI alias:

```bash
alias forge-admin-cli='kubectl exec -n forge-system deploy/carbide-api \
  -c carbide-api -- /opt/carbide/forge-admin-cli'
```

---

## 2. The Zero-DPU Problem (and Our Crude Fix)

The static IP branch handles BMC registration without DHCP — that part works
great. But the site explorer has a second gate: it counts BlueField DPUs via
Redfish and won't create a machine until all of them are DHCP-discovered.

On a GB200 NVL tray, Redfish reports 2 BlueField-3 DPUs per tray even if you're
not managing them. The site explorer sees "2 DPUs attached, 0 discovered" and
skips the host. The `allow_zero_dpu_hosts` config flag doesn't help — it only
kicks in when zero DPUs are *physically present*.

The 72x1 team ([Vinod](mailto:vchitrali@nvidia.com)) solved this by physically
disconnecting DPU cables so Redfish reports zero DPUs. We can't do that, so we
patch the code instead.

Two files need changes. Both are crude — they bypass checks that exist for good
reason in production. Fine for mini-rack bring-up.

### 2.1 Site Explorer — Let undiscovered DPUs through

**File**: `crates/api/src/site_explorer/mod.rs` (~line 1088)

The original code unconditionally skips any host with undiscovered DPUs:

```rust
if !dpu_added {
    if expected_num_dpus_attached_to_host > 0 {
        tracing::warn!("cannot identify managed host...");
        // ... power cycle logic ...
        continue;  // always skips — this is what blocks us
    } else if !self.config.allow_zero_dpu_hosts {
        continue;
    }
}
```

We add a check: if `allow_zero_dpu_hosts` is true, fall through instead of
skipping. The host gets treated as a zero-DPU machine:

```rust
if !dpu_added {
    if expected_num_dpus_attached_to_host > 0 {
        if self.config.allow_zero_dpu_hosts {
            tracing::warn!(
                address = %ep.address,
                "bypassing DPU discovery requirement: {} DPUs attached \
                 but none discovered via DHCP; proceeding as zero-DPU host",
                expected_num_dpus_attached_to_host
            );
            // fall through — host reaches create_zero_dpu_machine()
        } else {
            tracing::warn!("cannot identify managed host...");
            // ... existing power cycle logic unchanged ...
            continue;
        }
    } else if !self.config.allow_zero_dpu_hosts {
        continue;
    }
}
```

### 2.2 DHCP — Allow host NIC requests

**File**: `crates/api/src/dhcp/discover.rs` (~line 278)

Carbide rejects DHCP from the host NIC when an instance exists because it
assumes a DPU handles networking. For zero-DPU hosts, the host itself needs to
DHCP. Comment out the check:

```rust
// if let Some(machine_id) = machine_interface.machine_id {
//     if machine_id.machine_type().is_host()
//         && let Some(instance_id) =
//             db::instance::find_id_by_machine_id(&mut txn, &machine_id).await?
//     {
//         return Err(CarbideError::internal(format!(
//             "DHCP request received for instance: {instance_id}. Ignoring."
//         )));
//     }
// }
```

This comes from the 72x1 deployment ([rms-carbide commit `7aed855`](https://gitlab-master.nvidia.com/swserver/RackManagementService/rms-carbide/-/commit/7aed855588e6bcf1f3d7fd173b6219b92d8a9b35)).

---

## 3. Build and Deploy

### Build the binaries

```bash
cd ~/bare-metal-manager-core
export CARGO_HOME=$HOME/.cargo

# carbide-api (~5 min)
docker run --rm \
  --user $(id -u):$(id -g) \
  --volume $(pwd):/code --workdir /code \
  --volume $CARGO_HOME:/cargo --env CARGO_HOME=/cargo \
  "build-container-localdev" \
  carbide-api --release

# carbide-admin-cli (~2 min)
docker run --rm \
  --user $(id -u):$(id -g) \
  --volume $(pwd):/code --workdir /code \
  --volume $CARGO_HOME:/cargo --env CARGO_HOME=/cargo \
  "build-container-localdev" \
  carbide-admin-cli --release
```

`build-container-localdev` gets pulled from URM during `first_run.sh`. Its
entrypoint is `cargo build -p` so you just pass the crate name.

### Build the Docker image

```bash
mkdir -p /tmp/carbide-build
cp target/release/carbide-api target/release/carbide-admin-cli /tmp/carbide-build/

CURRENT_IMAGE=$(kubectl get deploy carbide-api -n forge-system \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="carbide-api")].image}')

cat <<EOF | docker build -t nvmetal-carbide:mini-rack-zero-dpu -f - /tmp/carbide-build
FROM $CURRENT_IMAGE
COPY carbide-api /opt/carbide/carbide-api
COPY carbide-admin-cli /opt/carbide/carbide-admin-cli
EOF
```

### Deploy

```bash
kubectl set image deployment/carbide-api -n forge-system \
  carbide-api=nvmetal-carbide:mini-rack-zero-dpu
kubectl rollout status deployment/carbide-api -n forge-system --timeout=120s
```

---

## 4. Configure

Edit the site config configmap:

```bash
kubectl edit configmap -n forge-system carbide-api-site-config-files
```

Under `[site_explorer]`:

```toml
[site_explorer]
allow_zero_dpu_hosts = true
create_machines = true
create_switches = true
explore_switches_from_static_ip = true
```

Restart to pick it up:

```bash
kubectl rollout restart deployment/carbide-api -n forge-system
```

---

## 5. Register Your Hardware

### BMC credentials

```bash
forge-admin-cli credential add-bmc --kind=site-wide-root \
  --username <bmc-user> --password <bmc-pass>
```

### Compute trays (with static IP)

```bash
forge-admin-cli expected-machine add \
  --bmc-mac-address <MAC> \
  --bmc-username <user> --bmc-password <pass> \
  --chassis-serial-number <serial> \
  --ip-address <BMC-IP> \
  --meta-name <hostname>
```

`--ip-address` is the whole point of @mrgalaxy 's the static IP branch. Repeat for each
compute tray.

### Switch (manual DB injection for static IP)

The `expected-switch add` CLI doesn't support `--ip-address` yet. Register
normally, then inject the IP into the DB.

```bash
forge-admin-cli expected-switch add \
  --bmc-mac-address <MAC> \
  --bmc-username <user> --bmc-password <pass> \
  --switch-serial-number <serial> \
  --nvos-username <nvos-user> --nvos-password <nvos-pass> \
  --meta-name <hostname>
```

Then inject the IP directly into postgres. The carbide database runs inside k3s
as a CrunchyData postgres cluster. !!!IMPORTANT To run SQL for all the following hacky commands:

```bash
kubectl exec -n postgres forge-pg-cluster-0 -c postgres -- \
  psql -U postgres -d forge_system_carbide -c "<SQL>"
```

This pattern is used for all DB operations in this guide — switch IP injection,
state machine nudges, etc. The database name is `forge_system_carbide` and the
`postgres` superuser works for everything we need.

Find your underlay segment:
```sql
SELECT id FROM network_segments WHERE network_segment_type = 'underlay';
```

Create the interface + address:
```sql
WITH new_iface AS (
  INSERT INTO machine_interfaces
    (id, mac_address, segment_id, primary_interface, hostname,
     is_static_ip, association_type)
  VALUES (gen_random_uuid(), '<SWITCH-BMC-MAC>', '<underlay-segment-id>',
          false, '<ip-slug>', true, 'None')
  RETURNING id
)
INSERT INTO machine_interface_addresses (interface_id, address)
SELECT id, '<SWITCH-BMC-IP>' FROM new_iface;
```

After the site explorer discovers the switch BMC, it may complain about missing
NVOS MAC addresses. Grab them from the endpoint report and update:

```sql
UPDATE expected_switches
SET nvos_mac_addresses = ARRAY['<MAC1>'::macaddr, '<MAC2>'::macaddr]
WHERE bmc_mac_address = '<SWITCH-BMC-MAC>';
```

If the switch gets stuck in error state, just set it to ready:
```sql
UPDATE switches SET controller_state = '{"state": "ready"}'
WHERE bmc_mac_address = '<SWITCH-BMC-MAC>';
```

---

## 6. Unstick the State Machine

This is where it gets hacky. Without forge-scout running on the
compute hosts, the carbide state machine gets stuck at two points waiting for
callbacks that will never arrive.

Watch `forge-admin-cli mh show` — machines will appear in
`HostInitializing/WaitingForDiscovery` after the site explorer runs (~60s
cycle). Then you nudge the DB.

### WaitingForDiscovery

The state machine waits for scout to call `discovery_completed`. Fake it with:

```sql
UPDATE machines SET last_discovery_time = NOW()
WHERE id IN ('<machine-id-1>', '<machine-id-2>');
```

Wait ~60s, check `mh show` again. The machines should advance through
UefiSetup, WaitingForLockdown, and into MachineValidation.

### MachineValidation

The state machine rebooted the host and is waiting for scout to confirm it came
back up. Fake the reboot and validation timestamps:

```sql
UPDATE machines
SET last_reboot_time = NOW(),
    last_discovery_time = NOW(),
    last_machine_validation_time = NOW()
WHERE id IN ('<machine-id-1>', '<machine-id-2>');
```

After another ~60s, machines should reach **Ready**. If BOM validation is
enabled, they'll pause at BomValidating — it should auto-skip if
`machine_validation_config.enabled = false` in your site config.

---

## 7. Verify

```bash
forge-admin-cli mh show                          # Ready
forge-admin-cli em show                          # Linked, with static IPs
forge-admin-cli switch show                      # Ready
forge-admin-cli ew show                          # IP visible, NVOS MACs set
forge-admin-cli mi show                          # all interfaces have IPs
forge-admin-cli site-explorer get-report endpoint # Complete for all BMCs
```

At this point carbide is managing your compute trays and switch. The machines
are in the DB, the BMCs are explored, and the state machine is happy. Have fun, don't make fun of...
