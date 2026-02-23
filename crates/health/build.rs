/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // gNMI proto definitions from OpenConfig (gnmi_service v0.10.0)
    // upstream: https://github.com/openconfig/gnmi/tree/master/proto
    let gnmi_proto_dir = "src/collectors/nvue/gnmi/proto";

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir(&out_dir)
        .compile_protos(
            &[format!("{gnmi_proto_dir}/gnmi_ext.proto")],
            &[gnmi_proto_dir.to_string()],
        )?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .extern_path(
            ".gnmi_ext",
            "crate::collectors::nvue::gnmi::proto::gnmi_ext",
        )
        .out_dir(&out_dir)
        .compile_protos(
            &[format!("{gnmi_proto_dir}/gnmi.proto")],
            &[gnmi_proto_dir.to_string()],
        )?;

    Ok(())
}
