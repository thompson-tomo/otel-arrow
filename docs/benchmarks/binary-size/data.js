window.BENCHMARK_DATA = {
  "lastUpdate": 1776913626162,
  "repoUrl": "https://github.com/thompson-tomo/otel-arrow",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b639b30ee6fb2e1453a80c71a0b29c018c233980",
          "message": "Columnar query engine support conditionally executing pipeline stages (#1684)\n\npart of #1667 \n\nAdds a new `ConditionalDataExpression` to the transformation expression\nAST for applying transformation `PipelineStages` to some subset of rows\nthat match a condition. This is used to implement a\n`ConditionalPipelineStage`, which operates like `if/else if/else`\ncontrol flow.\n\nFor example, imagine we had a hypothetical syntax like:\n```kql\nlogs |\n  if (severity_text == \"ERROR\") {\n     set attributes[\"important\"] = \"very\" | set attributes[\"triggers_alarm\"] = true\n  } else if (severity_text == \"WARN) {\n     set attributes[\"important\"] = \"somewhat\"\n  } else {\n     set attributes[\"important\"] = \"no\"\n  }\n```\n\nThis could be modeled using our conditional expression like:\n```rs\n// this is pesudocode to illustrate what each field represents\nConditional {\n  branches: [\n     ConditionalBranch {\n       condition: \"severity_text == \\\"ERROR\\\"\",\n       expressions: [ \n         \"set attributes[\\\"important\\\"] = \\\"very\\\"\",\n         \"set attributes[\\\"triggers_alarm\\\"] = true\"  \n      ],\n     },\n     ConditionalBranch {\n       condition: \"severity_text == \\\"WARN\\\"\",\n       expressions: [\n        \"set attributes[\\\"important\\\"] = \\\"somewhat\\\"\n      ],\n     },\n  ],\n  default_branch: Some([\n    \"set attributes[\"important\"] = \\\"no\\\"\"\n  ])\n}\n```\n\nNote there is currently no parser support for a language syntax that\ncreates this variant of `DataExpression`. That will happen in a future\nPR\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2025-12-23T23:56:20Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b639b30ee6fb2e1453a80c71a0b29c018c233980"
        },
        "date": 1766547146958,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b639b30ee6fb2e1453a80c71a0b29c018c233980",
          "message": "Columnar query engine support conditionally executing pipeline stages (#1684)\n\npart of #1667 \n\nAdds a new `ConditionalDataExpression` to the transformation expression\nAST for applying transformation `PipelineStages` to some subset of rows\nthat match a condition. This is used to implement a\n`ConditionalPipelineStage`, which operates like `if/else if/else`\ncontrol flow.\n\nFor example, imagine we had a hypothetical syntax like:\n```kql\nlogs |\n  if (severity_text == \"ERROR\") {\n     set attributes[\"important\"] = \"very\" | set attributes[\"triggers_alarm\"] = true\n  } else if (severity_text == \"WARN) {\n     set attributes[\"important\"] = \"somewhat\"\n  } else {\n     set attributes[\"important\"] = \"no\"\n  }\n```\n\nThis could be modeled using our conditional expression like:\n```rs\n// this is pesudocode to illustrate what each field represents\nConditional {\n  branches: [\n     ConditionalBranch {\n       condition: \"severity_text == \\\"ERROR\\\"\",\n       expressions: [ \n         \"set attributes[\\\"important\\\"] = \\\"very\\\"\",\n         \"set attributes[\\\"triggers_alarm\\\"] = true\"  \n      ],\n     },\n     ConditionalBranch {\n       condition: \"severity_text == \\\"WARN\\\"\",\n       expressions: [\n        \"set attributes[\\\"important\\\"] = \\\"somewhat\\\"\n      ],\n     },\n  ],\n  default_branch: Some([\n    \"set attributes[\"important\"] = \\\"no\\\"\"\n  ])\n}\n```\n\nNote there is currently no parser support for a language syntax that\ncreates this variant of `DataExpression`. That will happen in a future\nPR\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2025-12-23T23:56:20Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b639b30ee6fb2e1453a80c71a0b29c018c233980"
        },
        "date": 1766628406841,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.71,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.36,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b639b30ee6fb2e1453a80c71a0b29c018c233980",
          "message": "Columnar query engine support conditionally executing pipeline stages (#1684)\n\npart of #1667 \n\nAdds a new `ConditionalDataExpression` to the transformation expression\nAST for applying transformation `PipelineStages` to some subset of rows\nthat match a condition. This is used to implement a\n`ConditionalPipelineStage`, which operates like `if/else if/else`\ncontrol flow.\n\nFor example, imagine we had a hypothetical syntax like:\n```kql\nlogs |\n  if (severity_text == \"ERROR\") {\n     set attributes[\"important\"] = \"very\" | set attributes[\"triggers_alarm\"] = true\n  } else if (severity_text == \"WARN) {\n     set attributes[\"important\"] = \"somewhat\"\n  } else {\n     set attributes[\"important\"] = \"no\"\n  }\n```\n\nThis could be modeled using our conditional expression like:\n```rs\n// this is pesudocode to illustrate what each field represents\nConditional {\n  branches: [\n     ConditionalBranch {\n       condition: \"severity_text == \\\"ERROR\\\"\",\n       expressions: [ \n         \"set attributes[\\\"important\\\"] = \\\"very\\\"\",\n         \"set attributes[\\\"triggers_alarm\\\"] = true\"  \n      ],\n     },\n     ConditionalBranch {\n       condition: \"severity_text == \\\"WARN\\\"\",\n       expressions: [\n        \"set attributes[\\\"important\\\"] = \\\"somewhat\\\"\n      ],\n     },\n  ],\n  default_branch: Some([\n    \"set attributes[\"important\"] = \\\"no\\\"\"\n  ])\n}\n```\n\nNote there is currently no parser support for a language syntax that\ncreates this variant of `DataExpression`. That will happen in a future\nPR\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2025-12-23T23:56:20Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b639b30ee6fb2e1453a80c71a0b29c018c233980"
        },
        "date": 1766714754577,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8e81cc86d7a9574082e2c10d3bf4d2d02faa49b1",
          "message": "[query-engine] KQL function parsing + type conversion (#1668)\n\nRelates to #1479\n\n## Changes\n\n* Adds support for function definition and invocation in KQL parser\n* Implements automatic type conversion for functions in RecordSet engine",
          "timestamp": "2025-12-26T22:23:48Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8e81cc86d7a9574082e2c10d3bf4d2d02faa49b1"
        },
        "date": 1766800994512,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8e81cc86d7a9574082e2c10d3bf4d2d02faa49b1",
          "message": "[query-engine] KQL function parsing + type conversion (#1668)\n\nRelates to #1479\n\n## Changes\n\n* Adds support for function definition and invocation in KQL parser\n* Implements automatic type conversion for functions in RecordSet engine",
          "timestamp": "2025-12-26T22:23:48Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8e81cc86d7a9574082e2c10d3bf4d2d02faa49b1"
        },
        "date": 1766888184992,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.84,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8e81cc86d7a9574082e2c10d3bf4d2d02faa49b1",
          "message": "[query-engine] KQL function parsing + type conversion (#1668)\n\nRelates to #1479\n\n## Changes\n\n* Adds support for function definition and invocation in KQL parser\n* Implements automatic type conversion for functions in RecordSet engine",
          "timestamp": "2025-12-26T22:23:48Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8e81cc86d7a9574082e2c10d3bf4d2d02faa49b1"
        },
        "date": 1766974500037,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.86,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "66b4c7e30dca8c44340dace1056ed5a5887366ae",
          "message": "chore(deps): update dependency psutil to v7.2.1 (#1698)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n| [psutil](https://redirect.github.com/giampaolo/psutil) | `==7.1.3` ->\n`==7.2.1` |\n![age](https://developer.mend.io/api/mc/badges/age/pypi/psutil/7.2.1?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/pypi/psutil/7.1.3/7.2.1?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>giampaolo/psutil (psutil)</summary>\n\n###\n[`v7.2.1`](https://redirect.github.com/giampaolo/psutil/blob/HEAD/HISTORY.rst#721)\n\n[Compare\nSource](https://redirect.github.com/giampaolo/psutil/compare/release-7.2.0...release-7.2.1)\n\n\\=====\n\n2025-12-29\n\n**Bug fixes**\n\n- 2699\\_, \\[FreeBSD], \\[NetBSD]: `heap_info()`\\_ does not detect small\nallocations\n(<= 1K). In order to fix that, we now flush internal jemalloc cache\nbefore\n  fetching the metrics.\n\n###\n[`v7.2.0`](https://redirect.github.com/giampaolo/psutil/blob/HEAD/HISTORY.rst#720)\n\n[Compare\nSource](https://redirect.github.com/giampaolo/psutil/compare/release-7.1.3...release-7.2.0)\n\n\\=====\n\n2025-12-23\n\n**Enhancements**\n\n- 1275\\_: new `heap_info()`\\_ and `heap_trim()`\\_ functions, providing\ndirect\n  access to the platform's native C heap allocator (glibc, mimalloc,\n  libmalloc). Useful to create tools to detect memory leaks.\n- 2403\\_, \\[Linux]: publish wheels for Linux musl.\n- 2680\\_: unit tests are no longer installed / part of the distribution.\nThey\n  now live under `tests/` instead of `psutil/tests`.\n\n**Bug fixes**\n\n- 2684\\_, \\[FreeBSD], \\[critical]: compilation fails on FreeBSD 14 due\nto missing\n  include.\n- 2691\\_, \\[Windows]: fix memory leak in `net_if_stats()`\\_ due to\nmissing\n  `Py_CLEAR`.\n\n**Compatibility notes**\n\n- 2680\\_: `import psutil.tests` no longer works (but it was never\ndocumented to\n  begin with).\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - \"before 8am on Monday\" (UTC),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0Mi41OS4wIiwidXBkYXRlZEluVmVyIjoiNDIuNTkuMCIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2025-12-29T19:06:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/66b4c7e30dca8c44340dace1056ed5a5887366ae"
        },
        "date": 1767060388960,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.85,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1af4d28d2119f57dae33f166f635ef16d69223a5",
          "message": "feat: Initial Condense processor implementation (#1695)\n\nRelated to #1435\n\nFixes #1693\n\nThis is a basic implementation of the `Condense` behavior from the above\nissue that works for `LogAttrs` payload types.\n\nThis iteration currently builds an entirely new `RecordBatch` during\nexecution. As mentioned in comments, once #1035 is completed, working\nin-place on the existing `RecordBatch` would be more efficient\nespecially with respect to persisted attributes.\n\nWith a debug pipeline configuration composed of:\n* `syslog_cef_receiver`\n* `attributes_processor` (doing various renames and deletes)\n* `condense_attributes_processor`\n\nSending the CEF message:\n> <134>Dec 29 17:28:13 securityhost\nCEF:0|Security|threatmanager|1.0|100|worm successfully\nstopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp\nact=blocked vendorspecificext1=value1 vendorspecificext2=value2\n\nResults in the following LogRecord:\n```\nLogRecord #0:\n   -> ObservedTimestamp: 1767029293753398998\n   -> Timestamp: 1767029293000000000\n   -> SeverityText: INFO\n   -> SeverityNumber: 9\n   -> Body: <134>Dec 29 17:28:13 securityhost CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp act=blocked vendorspecificext1=value1 vendorspecificext2=value2\n   -> Attributes:\n      -> AdditionalExtensions: vendorspecificext1=value1|vendorspecificext2=value2\n      -> Computer: securityhost\n      -> DeviceVendor: Security\n      -> DeviceProduct: threatmanager\n      -> DeviceVersion: 1.0\n      -> DeviceEventClassId: 100\n      -> Activity: worm successfully stopped\n      -> LogSeverity: 10\n      -> SourceIP: 10.0.0.1\n      -> DestinationIP: 2.1.2.2\n      -> SourcePort: 1232\n      -> DestinationPort: 80\n      -> Protocol: tcp\n      -> DeviceAction: blocked\n   -> Trace ID:\n   -> Span ID:\n   -> Flags: 0 \n```",
          "timestamp": "2025-12-31T00:42:40Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1af4d28d2119f57dae33f166f635ef16d69223a5"
        },
        "date": 1767146839936,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.55,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1af4d28d2119f57dae33f166f635ef16d69223a5",
          "message": "feat: Initial Condense processor implementation (#1695)\n\nRelated to #1435\n\nFixes #1693\n\nThis is a basic implementation of the `Condense` behavior from the above\nissue that works for `LogAttrs` payload types.\n\nThis iteration currently builds an entirely new `RecordBatch` during\nexecution. As mentioned in comments, once #1035 is completed, working\nin-place on the existing `RecordBatch` would be more efficient\nespecially with respect to persisted attributes.\n\nWith a debug pipeline configuration composed of:\n* `syslog_cef_receiver`\n* `attributes_processor` (doing various renames and deletes)\n* `condense_attributes_processor`\n\nSending the CEF message:\n> <134>Dec 29 17:28:13 securityhost\nCEF:0|Security|threatmanager|1.0|100|worm successfully\nstopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp\nact=blocked vendorspecificext1=value1 vendorspecificext2=value2\n\nResults in the following LogRecord:\n```\nLogRecord #0:\n   -> ObservedTimestamp: 1767029293753398998\n   -> Timestamp: 1767029293000000000\n   -> SeverityText: INFO\n   -> SeverityNumber: 9\n   -> Body: <134>Dec 29 17:28:13 securityhost CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp act=blocked vendorspecificext1=value1 vendorspecificext2=value2\n   -> Attributes:\n      -> AdditionalExtensions: vendorspecificext1=value1|vendorspecificext2=value2\n      -> Computer: securityhost\n      -> DeviceVendor: Security\n      -> DeviceProduct: threatmanager\n      -> DeviceVersion: 1.0\n      -> DeviceEventClassId: 100\n      -> Activity: worm successfully stopped\n      -> LogSeverity: 10\n      -> SourceIP: 10.0.0.1\n      -> DestinationIP: 2.1.2.2\n      -> SourcePort: 1232\n      -> DestinationPort: 80\n      -> Protocol: tcp\n      -> DeviceAction: blocked\n   -> Trace ID:\n   -> Span ID:\n   -> Flags: 0 \n```",
          "timestamp": "2025-12-31T00:42:40Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1af4d28d2119f57dae33f166f635ef16d69223a5"
        },
        "date": 1767238554137,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.49,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1af4d28d2119f57dae33f166f635ef16d69223a5",
          "message": "feat: Initial Condense processor implementation (#1695)\n\nRelated to #1435\n\nFixes #1693\n\nThis is a basic implementation of the `Condense` behavior from the above\nissue that works for `LogAttrs` payload types.\n\nThis iteration currently builds an entirely new `RecordBatch` during\nexecution. As mentioned in comments, once #1035 is completed, working\nin-place on the existing `RecordBatch` would be more efficient\nespecially with respect to persisted attributes.\n\nWith a debug pipeline configuration composed of:\n* `syslog_cef_receiver`\n* `attributes_processor` (doing various renames and deletes)\n* `condense_attributes_processor`\n\nSending the CEF message:\n> <134>Dec 29 17:28:13 securityhost\nCEF:0|Security|threatmanager|1.0|100|worm successfully\nstopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp\nact=blocked vendorspecificext1=value1 vendorspecificext2=value2\n\nResults in the following LogRecord:\n```\nLogRecord #0:\n   -> ObservedTimestamp: 1767029293753398998\n   -> Timestamp: 1767029293000000000\n   -> SeverityText: INFO\n   -> SeverityNumber: 9\n   -> Body: <134>Dec 29 17:28:13 securityhost CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp act=blocked vendorspecificext1=value1 vendorspecificext2=value2\n   -> Attributes:\n      -> AdditionalExtensions: vendorspecificext1=value1|vendorspecificext2=value2\n      -> Computer: securityhost\n      -> DeviceVendor: Security\n      -> DeviceProduct: threatmanager\n      -> DeviceVersion: 1.0\n      -> DeviceEventClassId: 100\n      -> Activity: worm successfully stopped\n      -> LogSeverity: 10\n      -> SourceIP: 10.0.0.1\n      -> DestinationIP: 2.1.2.2\n      -> SourcePort: 1232\n      -> DestinationPort: 80\n      -> Protocol: tcp\n      -> DeviceAction: blocked\n   -> Trace ID:\n   -> Span ID:\n   -> Flags: 0 \n```",
          "timestamp": "2025-12-31T00:42:40Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1af4d28d2119f57dae33f166f635ef16d69223a5"
        },
        "date": 1767324210046,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.55,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1af4d28d2119f57dae33f166f635ef16d69223a5",
          "message": "feat: Initial Condense processor implementation (#1695)\n\nRelated to #1435\n\nFixes #1693\n\nThis is a basic implementation of the `Condense` behavior from the above\nissue that works for `LogAttrs` payload types.\n\nThis iteration currently builds an entirely new `RecordBatch` during\nexecution. As mentioned in comments, once #1035 is completed, working\nin-place on the existing `RecordBatch` would be more efficient\nespecially with respect to persisted attributes.\n\nWith a debug pipeline configuration composed of:\n* `syslog_cef_receiver`\n* `attributes_processor` (doing various renames and deletes)\n* `condense_attributes_processor`\n\nSending the CEF message:\n> <134>Dec 29 17:28:13 securityhost\nCEF:0|Security|threatmanager|1.0|100|worm successfully\nstopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp\nact=blocked vendorspecificext1=value1 vendorspecificext2=value2\n\nResults in the following LogRecord:\n```\nLogRecord #0:\n   -> ObservedTimestamp: 1767029293753398998\n   -> Timestamp: 1767029293000000000\n   -> SeverityText: INFO\n   -> SeverityNumber: 9\n   -> Body: <134>Dec 29 17:28:13 securityhost CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232 dpt=80 proto=tcp act=blocked vendorspecificext1=value1 vendorspecificext2=value2\n   -> Attributes:\n      -> AdditionalExtensions: vendorspecificext1=value1|vendorspecificext2=value2\n      -> Computer: securityhost\n      -> DeviceVendor: Security\n      -> DeviceProduct: threatmanager\n      -> DeviceVersion: 1.0\n      -> DeviceEventClassId: 100\n      -> Activity: worm successfully stopped\n      -> LogSeverity: 10\n      -> SourceIP: 10.0.0.1\n      -> DestinationIP: 2.1.2.2\n      -> SourcePort: 1232\n      -> DestinationPort: 80\n      -> Protocol: tcp\n      -> DeviceAction: blocked\n   -> Trace ID:\n   -> Span ID:\n   -> Flags: 0 \n```",
          "timestamp": "2025-12-31T00:42:40Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1af4d28d2119f57dae33f166f635ef16d69223a5"
        },
        "date": 1767405754826,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 79.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 67.49,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Andres Borja",
            "username": "andborja",
            "email": "76450334+andborja@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "2584e4b429b131115964742db4115782e6b19781",
          "message": "feat: Add internal telemetry prometheus exporter (#1691)\n\nAdd internal telemetry configurable prometheus exporter.",
          "timestamp": "2026-01-03T22:00:15Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2584e4b429b131115964742db4115782e6b19781"
        },
        "date": 1767493120287,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 80.72,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Andres Borja",
            "username": "andborja",
            "email": "76450334+andborja@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "2584e4b429b131115964742db4115782e6b19781",
          "message": "feat: Add internal telemetry prometheus exporter (#1691)\n\nAdd internal telemetry configurable prometheus exporter.",
          "timestamp": "2026-01-03T22:00:15Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2584e4b429b131115964742db4115782e6b19781"
        },
        "date": 1767579508366,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 80.71,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.18,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "4bf9bf85b4d0c2e7992e18e3dedda40427aa2485",
          "message": "[batch_processor] Support bytes-based batching via new `format = [otap|otlp|preserve]` (#1633)\n\nFixes #1570.\n\nAdds dual format configuration to batch processor, with separate\n`FormatConfig` structs for each payload format.\nThis supports forcing payload into one or the other format, or allowing\nboth to be preserved.\n\nThe new bytes-based batching routines operate by scanning through\ntop-level fields. Unlike the items-based batching mode, this may produce\nbatches that are less than the limit; like that mode, it can also\nproduce outputs greater than the limit.",
          "timestamp": "2026-01-06T01:10:06Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/4bf9bf85b4d0c2e7992e18e3dedda40427aa2485"
        },
        "date": 1767670330184,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.04,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Andres Borja",
            "username": "andborja",
            "email": "76450334+andborja@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "4d58ac8f2141e9fcc0baaa73cc7cdbeac38993eb",
          "message": "feat: Add 'tls' option to internal telemetry OTLP configuration (#1724)\n\nAdd 'tls' option to internal telemetry OTLP configuration with ca file.",
          "timestamp": "2026-01-06T21:31:56Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/4d58ac8f2141e9fcc0baaa73cc7cdbeac38993eb"
        },
        "date": 1767757268736,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.54,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9b0aa84cfc1ef5df2f4434a3290bab3abbd7299c",
          "message": "[kql_processor] Experimental KQL recordset processor (#1730)\n\nRelates to #1642\n\n## Changes\n\n* Adds a processor which executes KQL query using the RecordSet engine\n(OTLP-bytes form)\n\n## Details\n\n@drewrelmas took @jmacd's original work and added tests + config. I\ncleaned it up a bit and improved the bridge API to allow the processor\nto own the pipeline memory instead of storing it in a static. The static\npath is in place for callers needing to invoke things using FFI (from\nnon-Rust platforms).\n\n---------\n\nCo-authored-by: Drew Relmas <drewrelmas@microsoft.com>",
          "timestamp": "2026-01-08T00:49:19Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9b0aa84cfc1ef5df2f4434a3290bab3abbd7299c"
        },
        "date": 1767844220852,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7cffafe2cb2c3ac605852d8d87ba77b4a41b716c",
          "message": "Internal Telemetry Guidelines (#1727)\n\nThis PR defines a set of guidelines for our internal telemetry and for\ndescribing how we can establish a telemetry by design process.\n\nOnce this PR is merged, I will follow up with a series of PRs to align\nthe existing instrumentation with these recommendations.\n\n---------\n\nCo-authored-by: Cijo Thomas <cithomas@microsoft.com>",
          "timestamp": "2026-01-08T22:23:58Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7cffafe2cb2c3ac605852d8d87ba77b4a41b716c"
        },
        "date": 1767942374299,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "002f4368ddd47cc05a69bd93e39b7f27850d9bc7",
          "message": "Internal logging code path: Raw logger support (#1735)\n\nImplements new internal logging configuration option.\n\nChanges the default logging configuration to use internal logging at\nlevel INFO. Previously, default logging was disabled.\n\nImplements a lightweight Tokio tracing layer to construct\npartially-encoded OTLP bytes from the Event, forming a struct that can\nbe passed through a channel to a global subscriber.\n\nAs the first step, implements \"raw logging\" directly to the console\nusing simple write! macros and the view object for LogRecord to\ninterpret the partial encoding and print it. The raw logging limits\nconsole message size to 4KiB.\n\nAdds a new `configs/internal-telemetry.yaml` to demonstrate this\nconfiguration.\n\nAdds benchmarks showing good performance, in the 50-200ns range to\nencode or encode/format:\n\n```\nencode/0_attrs/100_events\n                        time:   [5.5326 µs 5.5691 µs 5.6054 µs]\n                        change: [−7.3098% −4.0342% −1.9226%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 1 outliers among 100 measurements (1.00%)\n  1 (1.00%) high mild\nencode/3_attrs/100_events\n                        time:   [8.5902 µs 8.6810 µs 8.7775 µs]\n                        change: [−5.7968% −3.2559% −1.1958%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 7 outliers among 100 measurements (7.00%)\n  2 (2.00%) low mild\n  2 (2.00%) high mild\n  3 (3.00%) high severe\nencode/10_attrs/100_events\n                        time:   [19.583 µs 19.764 µs 19.944 µs]\n                        change: [−1.5682% +0.0078% +1.3193%] (p = 0.99 > 0.05)\n                        No change in performance detected.\nFound 3 outliers among 100 measurements (3.00%)\n  3 (3.00%) high mild\nencode/0_attrs/1000_events\n                        time:   [53.424 µs 53.874 µs 54.289 µs]\n                        change: [−2.8602% −1.8582% −0.9413%] (p = 0.00 < 0.05)\n                        Change within noise threshold.\nFound 2 outliers among 100 measurements (2.00%)\n  1 (1.00%) low mild\n  1 (1.00%) high severe\nencode/3_attrs/1000_events\n                        time:   [84.768 µs 85.161 µs 85.562 µs]\n                        change: [−3.3406% −2.4035% −1.5473%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 5 outliers among 100 measurements (5.00%)\n  1 (1.00%) low mild\n  4 (4.00%) high mild\nencode/10_attrs/1000_events\n                        time:   [193.04 µs 194.07 µs 195.13 µs]\n                        change: [−1.8940% −0.1358% +1.7994%] (p = 0.89 > 0.05)\n                        No change in performance detected.\nFound 7 outliers among 100 measurements (7.00%)\n  1 (1.00%) low severe\n  1 (1.00%) low mild\n  2 (2.00%) high mild\n  3 (3.00%) high severe\n\nformat/0_attrs/100_events\n                        time:   [26.281 µs 26.451 µs 26.633 µs]\n                        change: [−16.944% −14.312% −10.992%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 6 outliers among 100 measurements (6.00%)\n  1 (1.00%) low mild\n  1 (1.00%) high mild\n  4 (4.00%) high severe\nformat/3_attrs/100_events\n                        time:   [38.813 µs 39.180 µs 39.603 µs]\n                        change: [−8.0880% −6.7812% −5.5109%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 8 outliers among 100 measurements (8.00%)\n  1 (1.00%) low severe\n  1 (1.00%) low mild\n  4 (4.00%) high mild\n  2 (2.00%) high severe\nformat/10_attrs/100_events\n                        time:   [70.655 µs 71.176 µs 71.752 µs]\n                        change: [−4.8840% −3.9457% −3.0096%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  4 (4.00%) high mild\nformat/0_attrs/1000_events\n                        time:   [295.80 µs 310.56 µs 325.75 µs]\n                        change: [−3.2629% −0.5673% +2.4337%] (p = 0.71 > 0.05)\n                        No change in performance detected.\nFound 10 outliers among 100 measurements (10.00%)\n  3 (3.00%) high mild\n  7 (7.00%) high severe\nformat/3_attrs/1000_events\n                        time:   [422.93 µs 430.42 µs 439.21 µs]\n                        change: [−1.3953% +0.8886% +3.3330%] (p = 0.46 > 0.05)\n                        No change in performance detected.\nFound 5 outliers among 100 measurements (5.00%)\n  5 (5.00%) high mild\nformat/10_attrs/1000_events\n                        time:   [720.96 µs 725.68 µs 730.81 µs]\n                        change: [−15.540% −13.383% −11.371%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 9 outliers among 100 measurements (9.00%)\n  1 (1.00%) low mild\n  5 (5.00%) high mild\n  3 (3.00%) high severe\n\nencode_and_format/0_attrs/100_events\n                        time:   [32.698 µs 32.914 µs 33.147 µs]\n                        change: [−9.4066% −7.8944% −6.3427%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 8 outliers among 100 measurements (8.00%)\n  2 (2.00%) low mild\n  3 (3.00%) high mild\n  3 (3.00%) high severe\nencode_and_format/3_attrs/100_events\n                        time:   [48.927 µs 49.498 µs 50.133 µs]\n                        change: [−7.2473% −5.1069% −2.7211%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 10 outliers among 100 measurements (10.00%)\n  3 (3.00%) high mild\n  7 (7.00%) high severe\nencode_and_format/10_attrs/100_events\n                        time:   [95.328 µs 96.088 µs 96.970 µs]\n                        change: [−6.3169% −4.9414% −3.6501%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 5 outliers among 100 measurements (5.00%)\n  4 (4.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/0_attrs/1000_events\n                        time:   [326.65 µs 328.86 µs 331.27 µs]\n                        change: [−41.188% −39.915% −38.764%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 7 outliers among 100 measurements (7.00%)\n  6 (6.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/3_attrs/1000_events\n                        time:   [500.59 µs 504.82 µs 509.33 µs]\n                        change: [−50.787% −48.877% −47.483%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  3 (3.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/10_attrs/1000_events\n                        time:   [944.34 µs 951.79 µs 960.38 µs]\n                        change: [−55.389% −54.741% −54.065%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  3 (3.00%) high mild\n  1 (1.00%) high severe\n```\n\n---------\n\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>",
          "timestamp": "2026-01-09T23:01:40Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/002f4368ddd47cc05a69bd93e39b7f27850d9bc7"
        },
        "date": 1768023993296,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.86,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "002f4368ddd47cc05a69bd93e39b7f27850d9bc7",
          "message": "Internal logging code path: Raw logger support (#1735)\n\nImplements new internal logging configuration option.\n\nChanges the default logging configuration to use internal logging at\nlevel INFO. Previously, default logging was disabled.\n\nImplements a lightweight Tokio tracing layer to construct\npartially-encoded OTLP bytes from the Event, forming a struct that can\nbe passed through a channel to a global subscriber.\n\nAs the first step, implements \"raw logging\" directly to the console\nusing simple write! macros and the view object for LogRecord to\ninterpret the partial encoding and print it. The raw logging limits\nconsole message size to 4KiB.\n\nAdds a new `configs/internal-telemetry.yaml` to demonstrate this\nconfiguration.\n\nAdds benchmarks showing good performance, in the 50-200ns range to\nencode or encode/format:\n\n```\nencode/0_attrs/100_events\n                        time:   [5.5326 µs 5.5691 µs 5.6054 µs]\n                        change: [−7.3098% −4.0342% −1.9226%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 1 outliers among 100 measurements (1.00%)\n  1 (1.00%) high mild\nencode/3_attrs/100_events\n                        time:   [8.5902 µs 8.6810 µs 8.7775 µs]\n                        change: [−5.7968% −3.2559% −1.1958%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 7 outliers among 100 measurements (7.00%)\n  2 (2.00%) low mild\n  2 (2.00%) high mild\n  3 (3.00%) high severe\nencode/10_attrs/100_events\n                        time:   [19.583 µs 19.764 µs 19.944 µs]\n                        change: [−1.5682% +0.0078% +1.3193%] (p = 0.99 > 0.05)\n                        No change in performance detected.\nFound 3 outliers among 100 measurements (3.00%)\n  3 (3.00%) high mild\nencode/0_attrs/1000_events\n                        time:   [53.424 µs 53.874 µs 54.289 µs]\n                        change: [−2.8602% −1.8582% −0.9413%] (p = 0.00 < 0.05)\n                        Change within noise threshold.\nFound 2 outliers among 100 measurements (2.00%)\n  1 (1.00%) low mild\n  1 (1.00%) high severe\nencode/3_attrs/1000_events\n                        time:   [84.768 µs 85.161 µs 85.562 µs]\n                        change: [−3.3406% −2.4035% −1.5473%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 5 outliers among 100 measurements (5.00%)\n  1 (1.00%) low mild\n  4 (4.00%) high mild\nencode/10_attrs/1000_events\n                        time:   [193.04 µs 194.07 µs 195.13 µs]\n                        change: [−1.8940% −0.1358% +1.7994%] (p = 0.89 > 0.05)\n                        No change in performance detected.\nFound 7 outliers among 100 measurements (7.00%)\n  1 (1.00%) low severe\n  1 (1.00%) low mild\n  2 (2.00%) high mild\n  3 (3.00%) high severe\n\nformat/0_attrs/100_events\n                        time:   [26.281 µs 26.451 µs 26.633 µs]\n                        change: [−16.944% −14.312% −10.992%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 6 outliers among 100 measurements (6.00%)\n  1 (1.00%) low mild\n  1 (1.00%) high mild\n  4 (4.00%) high severe\nformat/3_attrs/100_events\n                        time:   [38.813 µs 39.180 µs 39.603 µs]\n                        change: [−8.0880% −6.7812% −5.5109%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 8 outliers among 100 measurements (8.00%)\n  1 (1.00%) low severe\n  1 (1.00%) low mild\n  4 (4.00%) high mild\n  2 (2.00%) high severe\nformat/10_attrs/100_events\n                        time:   [70.655 µs 71.176 µs 71.752 µs]\n                        change: [−4.8840% −3.9457% −3.0096%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  4 (4.00%) high mild\nformat/0_attrs/1000_events\n                        time:   [295.80 µs 310.56 µs 325.75 µs]\n                        change: [−3.2629% −0.5673% +2.4337%] (p = 0.71 > 0.05)\n                        No change in performance detected.\nFound 10 outliers among 100 measurements (10.00%)\n  3 (3.00%) high mild\n  7 (7.00%) high severe\nformat/3_attrs/1000_events\n                        time:   [422.93 µs 430.42 µs 439.21 µs]\n                        change: [−1.3953% +0.8886% +3.3330%] (p = 0.46 > 0.05)\n                        No change in performance detected.\nFound 5 outliers among 100 measurements (5.00%)\n  5 (5.00%) high mild\nformat/10_attrs/1000_events\n                        time:   [720.96 µs 725.68 µs 730.81 µs]\n                        change: [−15.540% −13.383% −11.371%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 9 outliers among 100 measurements (9.00%)\n  1 (1.00%) low mild\n  5 (5.00%) high mild\n  3 (3.00%) high severe\n\nencode_and_format/0_attrs/100_events\n                        time:   [32.698 µs 32.914 µs 33.147 µs]\n                        change: [−9.4066% −7.8944% −6.3427%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 8 outliers among 100 measurements (8.00%)\n  2 (2.00%) low mild\n  3 (3.00%) high mild\n  3 (3.00%) high severe\nencode_and_format/3_attrs/100_events\n                        time:   [48.927 µs 49.498 µs 50.133 µs]\n                        change: [−7.2473% −5.1069% −2.7211%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 10 outliers among 100 measurements (10.00%)\n  3 (3.00%) high mild\n  7 (7.00%) high severe\nencode_and_format/10_attrs/100_events\n                        time:   [95.328 µs 96.088 µs 96.970 µs]\n                        change: [−6.3169% −4.9414% −3.6501%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 5 outliers among 100 measurements (5.00%)\n  4 (4.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/0_attrs/1000_events\n                        time:   [326.65 µs 328.86 µs 331.27 µs]\n                        change: [−41.188% −39.915% −38.764%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 7 outliers among 100 measurements (7.00%)\n  6 (6.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/3_attrs/1000_events\n                        time:   [500.59 µs 504.82 µs 509.33 µs]\n                        change: [−50.787% −48.877% −47.483%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  3 (3.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/10_attrs/1000_events\n                        time:   [944.34 µs 951.79 µs 960.38 µs]\n                        change: [−55.389% −54.741% −54.065%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  3 (3.00%) high mild\n  1 (1.00%) high severe\n```\n\n---------\n\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>",
          "timestamp": "2026-01-09T23:01:40Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/002f4368ddd47cc05a69bd93e39b7f27850d9bc7"
        },
        "date": 1768097922652,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.54,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 68.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "002f4368ddd47cc05a69bd93e39b7f27850d9bc7",
          "message": "Internal logging code path: Raw logger support (#1735)\n\nImplements new internal logging configuration option.\n\nChanges the default logging configuration to use internal logging at\nlevel INFO. Previously, default logging was disabled.\n\nImplements a lightweight Tokio tracing layer to construct\npartially-encoded OTLP bytes from the Event, forming a struct that can\nbe passed through a channel to a global subscriber.\n\nAs the first step, implements \"raw logging\" directly to the console\nusing simple write! macros and the view object for LogRecord to\ninterpret the partial encoding and print it. The raw logging limits\nconsole message size to 4KiB.\n\nAdds a new `configs/internal-telemetry.yaml` to demonstrate this\nconfiguration.\n\nAdds benchmarks showing good performance, in the 50-200ns range to\nencode or encode/format:\n\n```\nencode/0_attrs/100_events\n                        time:   [5.5326 µs 5.5691 µs 5.6054 µs]\n                        change: [−7.3098% −4.0342% −1.9226%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 1 outliers among 100 measurements (1.00%)\n  1 (1.00%) high mild\nencode/3_attrs/100_events\n                        time:   [8.5902 µs 8.6810 µs 8.7775 µs]\n                        change: [−5.7968% −3.2559% −1.1958%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 7 outliers among 100 measurements (7.00%)\n  2 (2.00%) low mild\n  2 (2.00%) high mild\n  3 (3.00%) high severe\nencode/10_attrs/100_events\n                        time:   [19.583 µs 19.764 µs 19.944 µs]\n                        change: [−1.5682% +0.0078% +1.3193%] (p = 0.99 > 0.05)\n                        No change in performance detected.\nFound 3 outliers among 100 measurements (3.00%)\n  3 (3.00%) high mild\nencode/0_attrs/1000_events\n                        time:   [53.424 µs 53.874 µs 54.289 µs]\n                        change: [−2.8602% −1.8582% −0.9413%] (p = 0.00 < 0.05)\n                        Change within noise threshold.\nFound 2 outliers among 100 measurements (2.00%)\n  1 (1.00%) low mild\n  1 (1.00%) high severe\nencode/3_attrs/1000_events\n                        time:   [84.768 µs 85.161 µs 85.562 µs]\n                        change: [−3.3406% −2.4035% −1.5473%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 5 outliers among 100 measurements (5.00%)\n  1 (1.00%) low mild\n  4 (4.00%) high mild\nencode/10_attrs/1000_events\n                        time:   [193.04 µs 194.07 µs 195.13 µs]\n                        change: [−1.8940% −0.1358% +1.7994%] (p = 0.89 > 0.05)\n                        No change in performance detected.\nFound 7 outliers among 100 measurements (7.00%)\n  1 (1.00%) low severe\n  1 (1.00%) low mild\n  2 (2.00%) high mild\n  3 (3.00%) high severe\n\nformat/0_attrs/100_events\n                        time:   [26.281 µs 26.451 µs 26.633 µs]\n                        change: [−16.944% −14.312% −10.992%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 6 outliers among 100 measurements (6.00%)\n  1 (1.00%) low mild\n  1 (1.00%) high mild\n  4 (4.00%) high severe\nformat/3_attrs/100_events\n                        time:   [38.813 µs 39.180 µs 39.603 µs]\n                        change: [−8.0880% −6.7812% −5.5109%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 8 outliers among 100 measurements (8.00%)\n  1 (1.00%) low severe\n  1 (1.00%) low mild\n  4 (4.00%) high mild\n  2 (2.00%) high severe\nformat/10_attrs/100_events\n                        time:   [70.655 µs 71.176 µs 71.752 µs]\n                        change: [−4.8840% −3.9457% −3.0096%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  4 (4.00%) high mild\nformat/0_attrs/1000_events\n                        time:   [295.80 µs 310.56 µs 325.75 µs]\n                        change: [−3.2629% −0.5673% +2.4337%] (p = 0.71 > 0.05)\n                        No change in performance detected.\nFound 10 outliers among 100 measurements (10.00%)\n  3 (3.00%) high mild\n  7 (7.00%) high severe\nformat/3_attrs/1000_events\n                        time:   [422.93 µs 430.42 µs 439.21 µs]\n                        change: [−1.3953% +0.8886% +3.3330%] (p = 0.46 > 0.05)\n                        No change in performance detected.\nFound 5 outliers among 100 measurements (5.00%)\n  5 (5.00%) high mild\nformat/10_attrs/1000_events\n                        time:   [720.96 µs 725.68 µs 730.81 µs]\n                        change: [−15.540% −13.383% −11.371%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 9 outliers among 100 measurements (9.00%)\n  1 (1.00%) low mild\n  5 (5.00%) high mild\n  3 (3.00%) high severe\n\nencode_and_format/0_attrs/100_events\n                        time:   [32.698 µs 32.914 µs 33.147 µs]\n                        change: [−9.4066% −7.8944% −6.3427%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 8 outliers among 100 measurements (8.00%)\n  2 (2.00%) low mild\n  3 (3.00%) high mild\n  3 (3.00%) high severe\nencode_and_format/3_attrs/100_events\n                        time:   [48.927 µs 49.498 µs 50.133 µs]\n                        change: [−7.2473% −5.1069% −2.7211%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 10 outliers among 100 measurements (10.00%)\n  3 (3.00%) high mild\n  7 (7.00%) high severe\nencode_and_format/10_attrs/100_events\n                        time:   [95.328 µs 96.088 µs 96.970 µs]\n                        change: [−6.3169% −4.9414% −3.6501%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 5 outliers among 100 measurements (5.00%)\n  4 (4.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/0_attrs/1000_events\n                        time:   [326.65 µs 328.86 µs 331.27 µs]\n                        change: [−41.188% −39.915% −38.764%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 7 outliers among 100 measurements (7.00%)\n  6 (6.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/3_attrs/1000_events\n                        time:   [500.59 µs 504.82 µs 509.33 µs]\n                        change: [−50.787% −48.877% −47.483%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  3 (3.00%) high mild\n  1 (1.00%) high severe\nencode_and_format/10_attrs/1000_events\n                        time:   [944.34 µs 951.79 µs 960.38 µs]\n                        change: [−55.389% −54.741% −54.065%] (p = 0.00 < 0.05)\n                        Performance has improved.\nFound 4 outliers among 100 measurements (4.00%)\n  3 (3.00%) high mild\n  1 (1.00%) high severe\n```\n\n---------\n\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>",
          "timestamp": "2026-01-09T23:01:40Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/002f4368ddd47cc05a69bd93e39b7f27850d9bc7"
        },
        "date": 1768190331169,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.71,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.05,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9f7b097b8801e7c3ed037064d50c443ec9c1e13f",
          "message": "fix(deps): update golang.org/x/exp digest to 716be56 (#1767)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| [golang.org/x/exp](https://pkg.go.dev/golang.org/x/exp) | require |\ndigest | `944ab1f` → `716be56` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - At any time (no schedule defined),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0Mi43NC41IiwidXBkYXRlZEluVmVyIjoiNDIuNzQuNSIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\n---------\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: otelbot <197425009+otelbot@users.noreply.github.com>\nCo-authored-by: Drew Relmas <drewrelmas@gmail.com>",
          "timestamp": "2026-01-13T00:34:21Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9f7b097b8801e7c3ed037064d50c443ec9c1e13f"
        },
        "date": 1768277183602,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.71,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.11,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb",
            "email": "lalit_fin@yahoo.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "f72798b2f168c0a7c2f469533ade55e6b1bd07c3",
          "message": "docs: Add architecture and configuration doc for mTLS/TLS for exporter and receiver.  (#1773)\n\nAdds comprehensive documentation for TLS/mTLS support in OTLP/OTAP\nreceivers and exporters.\n\n  ## Changes\n\n- **Configuration Guide**: User-facing documentation covering TLS/mTLS\nsetup, certificate hot-reload, configuration examples, security best\npractices, and troubleshooting\n- **Architecture Guide**: Developer-focused documentation covering\ndesign principles, component architecture, certificate reload\nmechanisms, performance characteristics, and future enhancements\n\nNote - Documentation was drafted using LLM , and then I validated\nagainst the code to ensure it is consistent.\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>",
          "timestamp": "2026-01-13T22:57:12Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f72798b2f168c0a7c2f469533ade55e6b1bd07c3"
        },
        "date": 1768364431688,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.72,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.11,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Gokhan Uslu",
            "username": "gouslu",
            "email": "geukhanuslu@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "4b646461dc3070dbe85c5cbc3051ddd08d7331f3",
          "message": "start using thiserror instead of string to avoid using format (#1787)",
          "timestamp": "2026-01-15T00:27:58Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/4b646461dc3070dbe85c5cbc3051ddd08d7331f3"
        },
        "date": 1768449133542,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.08,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "94af57b4abe8ecb93838572f259645cc6ea9b5a7",
          "message": "Scale and Saturation test update (#1788)\n\nLocal run output is shown below. The same is uploaded to usual charts,\nso we can see how linearly we scale with CPU cores.\n\nThe saturation-tests will be refactored in future, to focus just on the\nscaling aspects (and probably renamed as scaling-tests).\n\n\n```txt\n==============================================\nAnalyzing Scaling Efficiency\n==============================================\n\nFound: 1 core(s) -> 181,463 logs/sec\nFound: 2 core(s) -> 257,679 logs/sec\nFound: 4 core(s) -> 454,159 logs/sec\n\n================================================================================\nSATURATION/SCALING TEST RESULTS - SCALING ANALYSIS\n================================================================================\n\nGoal: Verify shared-nothing architecture with linear CPU scaling\nBaseline (1 core): 181,463 logs/sec\n\n--------------------------------------------------------------------------------\nCores    Throughput (logs/sec)     Expected (linear)    Scaling Efficiency\n--------------------------------------------------------------------------------\n1        181,463                   181,463              100.00% ✅\n2        257,679                   362,927              71.00% 🟠\n4        454,159                   725,853              62.57% 🔴\n--------------------------------------------------------------------------------\n\nSUMMARY:\n  • Average Scaling Efficiency: 77.86%\n  • Minimum Scaling Efficiency: 62.57%\n  • Maximum Throughput (4 cores): 454,159 logs/sec\n  • Speedup (4 cores vs 1 core): 2.5x\n\n🟠 ACCEPTABLE: The engine shows reasonable scaling.\n   Some contention or overhead present.\n\n================================================================================\n```\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-01-15T23:41:59Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/94af57b4abe8ecb93838572f259645cc6ea9b5a7"
        },
        "date": 1768529442688,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.1,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.49,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c68e70eda406b6341cbd0ae73cf4521a56639d47",
          "message": "Update batch size variation perf tests (#1809)\n\nModified to use 10, 100, 512, 1024, 4096, 8192 as sizes.\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-01-16T23:41:49Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c68e70eda406b6341cbd0ae73cf4521a56639d47"
        },
        "date": 1768625910338,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.14,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.49,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8470d2d442782c9e6dadf2b9379160f88ccc2c39",
          "message": "Split opentelemetry_client into otel_sdk, tracing_init, and ITS parts (#1808)\n\nPart of https://github.com/open-telemetry/otel-arrow/pull/1771.\n\nPart of https://github.com/open-telemetry/otel-arrow/issues/1736.\n\nThis is a non-functional refactoring of `opentelemetry_client.rs` into\nother places. This will make it clearer what changes in #1771 and what\nis just moving around.\n\nMoves runtime elements into the InternalTelemetrySystem, simplifies\nsetup for the controller where logs/metrics were separated.\n\nMoves OTel-SDK specific pieces into `otel_sdk` module, separates the\nTokio `tracing` setup.\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-01-17T02:49:23Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8470d2d442782c9e6dadf2b9379160f88ccc2c39"
        },
        "date": 1768702636322,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.12,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.49,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d8a0d6d381f1a9f2968c182c88920cb4ded93cc0",
          "message": "Create entity and expose entity keys via thread locals and task locals (#1785)\n\nThe engine now creates the following entities:\n\n* **Pipeline** -> Stored in a thread local associated with the pipeline\nthread.\n* **Node** -> Stored in the task local of the node.\n* **Channel**\n  * **Sender entity** stored in the task local of the sender node.\n  * **Receiver entity** stored in the task local of the receiver node.\n\nAn entity cleanup mechanism is in place. A unit test has been added to\nvalidate this cleanup process.\n\nThe final goal is to be able to use these entities directly when\nreporting metric sets and events. This allows us to report the\nattributes of all our entities using a simple numerical ID.\n\nCloses https://github.com/open-telemetry/otel-arrow/issues/1791",
          "timestamp": "2026-01-18T07:23:23Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d8a0d6d381f1a9f2968c182c88920cb4ded93cc0"
        },
        "date": 1768788932313,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.62,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "86b03dcd2ab9007d29e7cb0de6d1fcf86c9ead6b",
          "message": "PerfTest - include OTAP to OTAP in saturation/scaling test (#1815)\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-01-19T21:43:46Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/86b03dcd2ab9007d29e7cb0de6d1fcf86c9ead6b"
        },
        "date": 1768881908489,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.29,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.62,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "e4c170b3704bac31d91ff764fd8ad9eb2dad51e3",
          "message": "Replace uses of log:: with otel_ macros in crates/engine, crates/otap (#1843)\n\nPart of https://github.com/open-telemetry/otel-arrow/pull/1771.\n\nPart of https://github.com/open-telemetry/otel-arrow/issues/1736.\n\nOverlaps with #1841 by copying the file\ncrates/telemetry/src/internal_events.rs to extend the otel_xxx macros to\nfull Tokio syntax, to replace uses of log formatting as needed.\n\nAfter this, #1841 can remove \"log\" from the workspace Cargo.toml b/c\ncrates/state will have the remaining \"log\" references fixed there.",
          "timestamp": "2026-01-20T23:18:00Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/e4c170b3704bac31d91ff764fd8ad9eb2dad51e3"
        },
        "date": 1768975607768,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.67,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "2fae2f7dafe88504069a439a3d9ef89ea49f09ff",
          "message": "Console exporter for OTAP/OTLP logs (#1849)\n\nPart of https://github.com/open-telemetry/otel-arrow/pull/1771.\n\nPart of https://github.com/open-telemetry/otel-arrow/issues/1736.\n\nUses the new internal logging support to format OTLP logs data. This\nprints RESOURCE and SCOPE lines with ASCII or Unicode pipe structures to\nidentify the OTLP hierarchy:\n\n```\n2026-01-21T03:12:22.165Z  RESOURCE   v1.Resource: [fake_data_generator=v1]\n2026-01-21T03:12:22.165Z  │ SCOPE    v1.InstrumentationScope:\n2026-01-21T03:12:22.165Z  │ ├─ INFO  session.start:  [session.id=00112233-4455-6677-8899-aabbccddeeff, session.previous_id=00112233-4455-6677-8899-aabbccddeeff]\n2026-01-21T03:12:22.165Z  │ ├─ INFO  session.end:  [session.id=00112233-4455-6677-8899-aabbccddeeff]\n2026-01-21T03:12:22.165Z  │ ├─ INFO  device.app.lifecycle:  [ios.app.state=active, android.app.state=created]\n2026-01-21T03:12:22.165Z  │ ├─ INFO  rpc.message:  [rpc.message.type=SENT, rpc.message.id=42, rpc.message.compressed_size=42, rpc.message.uncompressed_size=42]\n```\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-01-22T04:56:14Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2fae2f7dafe88504069a439a3d9ef89ea49f09ff"
        },
        "date": 1769083932027,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.73,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.99,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Aaron Marten",
            "username": "AaronRM",
            "email": "AaronRM@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "cc1a2a2eaefa8d6af587fc054e9099333748b2c4",
          "message": "[otap-df-quiver] Track highest segment sequence during startup to avoid ID reuse (#1856)\n\n# Change Summary\n\nFixes #1855 by ensuring the Quiver engine scans existing segment files\nand starts with the next sequence number on recovery.\n\n## What issue does this PR close?\n\n* Closes #1855 \n\n## How are these changes tested?\n\nAdd `restart_recovers_segment_sequence_numbers` to validate that the\nissue is resolved & manual validation\n\n## Are there any user-facing changes?\n\nNo.",
          "timestamp": "2026-01-22T17:26:08Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/cc1a2a2eaefa8d6af587fc054e9099333748b2c4"
        },
        "date": 1769140863228,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 80.73,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.05,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c9f4e5d1a3249bebfe87f9d1d74c7d91f2ef171b",
          "message": "Add few logs to various components to expose shutdown issue (#1869)\n\n# Change Summary\n\nAdds/improves few internal logs to make the engine more observable. \n\n## How are these changes tested?\n\nLocal, manual runs\n\n## Are there any user-facing changes?\n\nBetter logs!",
          "timestamp": "2026-01-23T00:01:10Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c9f4e5d1a3249bebfe87f9d1d74c7d91f2ef171b"
        },
        "date": 1769231150437,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 80.86,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.12,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "58180138e1dfd8118f9b7d39cad784272aa50b74",
          "message": "perftest- temporarily remove saturation test with 24 core (#1884)\n\nI am observing similar issue to\nhttps://github.com/open-telemetry/otel-arrow/issues/1870 in the OTLP to\nOTLP scenario in loadtest - for the 24 core SUT, we use 72 core\nLoad-generator, and the load-generator is not shutting down properly. It\nis entirely possible that 72 pipelines instances would need more time to\nshutdown; until this can be investigated, its best to temporarily remove\nthis scenario.\n\nTo unblock perf tests, disabling the 24 core test temporarily. I'll\ninvestigate a proper fix next week.",
          "timestamp": "2026-01-24T18:42:13Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/58180138e1dfd8118f9b7d39cad784272aa50b74"
        },
        "date": 1769307881499,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.04,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "58180138e1dfd8118f9b7d39cad784272aa50b74",
          "message": "perftest- temporarily remove saturation test with 24 core (#1884)\n\nI am observing similar issue to\nhttps://github.com/open-telemetry/otel-arrow/issues/1870 in the OTLP to\nOTLP scenario in loadtest - for the 24 core SUT, we use 72 core\nLoad-generator, and the load-generator is not shutting down properly. It\nis entirely possible that 72 pipelines instances would need more time to\nshutdown; until this can be investigated, its best to temporarily remove\nthis scenario.\n\nTo unblock perf tests, disabling the 24 core test temporarily. I'll\ninvestigate a proper fix next week.",
          "timestamp": "2026-01-24T18:42:13Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/58180138e1dfd8118f9b7d39cad784272aa50b74"
        },
        "date": 1769393989618,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.04,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "3f0c85c4d65a91562de3165088edececc378f0eb",
          "message": "fix(deps): update module go.opentelemetry.io/collector/pdata to v1.50.0 (#1890)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n|\n[go.opentelemetry.io/collector/pdata](https://redirect.github.com/open-telemetry/opentelemetry-collector)\n| `v1.49.0` → `v1.50.0` |\n![age](https://developer.mend.io/api/mc/badges/age/go/go.opentelemetry.io%2fcollector%2fpdata/v1.50.0?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/go/go.opentelemetry.io%2fcollector%2fpdata/v1.49.0/v1.50.0?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>open-telemetry/opentelemetry-collector\n(go.opentelemetry.io/collector/pdata)</summary>\n\n###\n[`v1.50.0`](https://redirect.github.com/open-telemetry/opentelemetry-collector/blob/HEAD/CHANGELOG.md#v1500v01440)\n\n##### 🛑 Breaking changes 🛑\n\n- `pkg/exporterhelper`: Change verbosity level for\notelcol\\_exporter\\_queue\\_batch\\_send\\_size metric to detailed.\n([#&#8203;14278](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14278))\n- `pkg/service`: Remove deprecated\n`telemetry.disableHighCardinalityMetrics` feature gate.\n([#&#8203;14373](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14373))\n- `pkg/service`: Remove deprecated `service.noopTracerProvider` feature\ngate.\n([#&#8203;14374](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14374))\n\n##### 🚩 Deprecations 🚩\n\n- `exporter/otlp_grpc`: Rename `otlp` exporter to `otlp_grpc` exporter\nand add deprecated alias `otlp`.\n([#&#8203;14403](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14403))\n- `exporter/otlp_http`: Rename `otlphttp` exporter to `otlp_http`\nexporter and add deprecated alias `otlphttp`.\n([#&#8203;14396](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14396))\n\n##### 💡 Enhancements 💡\n\n- `cmd/builder`: Avoid duplicate CLI error logging in generated\ncollector binaries by relying on cobra's error handling.\n([#&#8203;14317](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14317))\n\n- `cmd/mdatagen`: Add the ability to disable attributes at the metric\nlevel and re-aggregate data points based off of these new dimensions\n([#&#8203;10726](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/10726))\n\n- `cmd/mdatagen`: Add optional `display_name` and `description` fields\nto metadata.yaml for human-readable component names\n([#&#8203;14114](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14114))\nThe `display_name` field allows components to specify a human-readable\nname in metadata.yaml.\nWhen provided, this name is used as the title in generated README files.\nThe `description` field allows components to include a brief description\nin generated README files.\n\n- `cmd/mdatagen`: Validate stability level for entities\n([#&#8203;14425](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14425))\n\n- `pkg/xexporterhelper`: Reenable batching for profiles\n([#&#8203;14313](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14313))\n\n- `receiver/nop`: add profiles signal support\n([#&#8203;14253](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14253))\n\n##### 🧰 Bug fixes 🧰\n\n- `pkg/exporterhelper`: Fix reference count bug in partition batcher\n([#&#8203;14444](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14444))\n\n<!-- previous-version -->\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - \"before 8am on Monday\" (UTC),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0Mi45Mi4xIiwidXBkYXRlZEluVmVyIjoiNDIuOTIuMSIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\n---------\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: otelbot <197425009+otelbot@users.noreply.github.com>\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-01-26T16:09:46Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/3f0c85c4d65a91562de3165088edececc378f0eb"
        },
        "date": 1769485776799,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "e1c7a802b626d7c8a6061e9f1a3ced60ac9417eb",
          "message": "fix(deps): update all patch versions (#1894)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n| [duckdb](https://redirect.github.com/duckdb/duckdb-python)\n([changelog](https://redirect.github.com/duckdb/duckdb-python/releases))\n| `==1.4.3` → `==1.4.4` |\n![age](https://developer.mend.io/api/mc/badges/age/pypi/duckdb/1.4.4?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/pypi/duckdb/1.4.3/1.4.4?slim=true)\n|\n|\n[github.com/apache/arrow-go/v18](https://redirect.github.com/apache/arrow-go)\n| `v18.5.0` → `v18.5.1` |\n![age](https://developer.mend.io/api/mc/badges/age/go/github.com%2fapache%2farrow-go%2fv18/v18.5.1?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/go/github.com%2fapache%2farrow-go%2fv18/v18.5.0/v18.5.1?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>duckdb/duckdb-python (duckdb)</summary>\n\n###\n[`v1.4.4`](https://redirect.github.com/duckdb/duckdb-python/releases/tag/v1.4.4):\nBugfix Release\n\n[Compare\nSource](https://redirect.github.com/duckdb/duckdb-python/compare/v1.4.3...v1.4.4)\n\n**DuckDB core v1.4.4 Changelog**:\n<https://github.com/duckdb/duckdb/compare/v1.4.3...v1.4.4>\n\n#### What's Changed in the Python Extension\n\n- fix polars tests by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;218](https://redirect.github.com/duckdb/duckdb-python/pull/218)\n- tests for string and binary views by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;221](https://redirect.github.com/duckdb/duckdb-python/pull/221)\n- Quote view names in unregister by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;222](https://redirect.github.com/duckdb/duckdb-python/pull/222)\n- Limit string nodes in Polars expressions to constant expressions by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;225](https://redirect.github.com/duckdb/duckdb-python/pull/225)\n- Escape identifiers in relation aggregations by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;272](https://redirect.github.com/duckdb/duckdb-python/pull/272)\n- Fix DECREF bug during interpreter shutdown by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;275](https://redirect.github.com/duckdb/duckdb-python/pull/275)\n- Support for Pandas 3.0.0 by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;277](https://redirect.github.com/duckdb/duckdb-python/pull/277)\n- Prepare for v1.4.4 by\n[@&#8203;evertlammerts](https://redirect.github.com/evertlammerts) in\n[#&#8203;280](https://redirect.github.com/duckdb/duckdb-python/pull/280)\n\n**Full Changelog**:\n<https://github.com/duckdb/duckdb-python/compare/v1.4.3...v1.4.4>\n\n</details>\n\n<details>\n<summary>apache/arrow-go (github.com/apache/arrow-go/v18)</summary>\n\n###\n[`v18.5.1`](https://redirect.github.com/apache/arrow-go/releases/tag/v18.5.1)\n\n[Compare\nSource](https://redirect.github.com/apache/arrow-go/compare/v18.5.0...v18.5.1)\n\n#### What's Changed\n\n- fix(internal): fix assertion on undefined behavior by\n[@&#8203;amoeba](https://redirect.github.com/amoeba) in\n[#&#8203;602](https://redirect.github.com/apache/arrow-go/pull/602)\n- chore: Bump actions/upload-artifact from 5.0.0 to 6.0.0 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;611](https://redirect.github.com/apache/arrow-go/pull/611)\n- chore: Bump google.golang.org/protobuf from 1.36.10 to 1.36.11 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;607](https://redirect.github.com/apache/arrow-go/pull/607)\n- chore: Bump github.com/pierrec/lz4/v4 from 4.1.22 to 4.1.23 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;616](https://redirect.github.com/apache/arrow-go/pull/616)\n- chore: Bump golang.org/x/tools from 0.39.0 to 0.40.0 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;609](https://redirect.github.com/apache/arrow-go/pull/609)\n- chore: Bump actions/cache from 4 to 5 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;608](https://redirect.github.com/apache/arrow-go/pull/608)\n- chore: Bump actions/download-artifact from 6.0.0 to 7.0.0 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;610](https://redirect.github.com/apache/arrow-go/pull/610)\n- ci(benchmark): switch to new conbench instance by\n[@&#8203;rok](https://redirect.github.com/rok) in\n[#&#8203;593](https://redirect.github.com/apache/arrow-go/pull/593)\n- fix(flight): make StreamChunksFromReader ctx aware and\ncancellation-safe by\n[@&#8203;arnoldwakim](https://redirect.github.com/arnoldwakim) in\n[#&#8203;615](https://redirect.github.com/apache/arrow-go/pull/615)\n- fix(parquet/variant): fix basic stringify by\n[@&#8203;zeroshade](https://redirect.github.com/zeroshade) in\n[#&#8203;624](https://redirect.github.com/apache/arrow-go/pull/624)\n- chore: Bump github.com/google/flatbuffers from 25.9.23+incompatible to\n25.12.19+incompatible by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;617](https://redirect.github.com/apache/arrow-go/pull/617)\n- chore: Bump google.golang.org/grpc from 1.77.0 to 1.78.0 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;621](https://redirect.github.com/apache/arrow-go/pull/621)\n- chore: Bump golang.org/x/tools from 0.40.0 to 0.41.0 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;626](https://redirect.github.com/apache/arrow-go/pull/626)\n- fix(parquet/pqarrow): fix partial struct panic by\n[@&#8203;zeroshade](https://redirect.github.com/zeroshade) in\n[#&#8203;630](https://redirect.github.com/apache/arrow-go/pull/630)\n- Flaky test fixes by\n[@&#8203;zeroshade](https://redirect.github.com/zeroshade) in\n[#&#8203;629](https://redirect.github.com/apache/arrow-go/pull/629)\n- ipc: clear variadicCounts in recordEncoder.reset() by\n[@&#8203;asubiotto](https://redirect.github.com/asubiotto) in\n[#&#8203;631](https://redirect.github.com/apache/arrow-go/pull/631)\n- fix(arrow/cdata): Handle errors to prevent panic by\n[@&#8203;xiaocai2333](https://redirect.github.com/xiaocai2333) in\n[#&#8203;614](https://redirect.github.com/apache/arrow-go/pull/614)\n- chore: Bump github.com/substrait-io/substrait-go/v7 from 7.2.0 to\n7.2.2 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;612](https://redirect.github.com/apache/arrow-go/pull/612)\n- chore: bump version to 18.5.1 by\n[@&#8203;zeroshade](https://redirect.github.com/zeroshade) in\n[#&#8203;632](https://redirect.github.com/apache/arrow-go/pull/632)\n\n#### New Contributors\n\n- [@&#8203;rok](https://redirect.github.com/rok) made their first\ncontribution in\n[#&#8203;593](https://redirect.github.com/apache/arrow-go/pull/593)\n- [@&#8203;asubiotto](https://redirect.github.com/asubiotto) made their\nfirst contribution in\n[#&#8203;631](https://redirect.github.com/apache/arrow-go/pull/631)\n- [@&#8203;xiaocai2333](https://redirect.github.com/xiaocai2333) made\ntheir first contribution in\n[#&#8203;614](https://redirect.github.com/apache/arrow-go/pull/614)\n\n**Full Changelog**:\n<https://github.com/apache/arrow-go/compare/v18.5.0...v18.5.1>\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - \"before 8am every weekday\" (UTC),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n👻 **Immortal**: This PR will be recreated if closed unmerged. Get\n[config\nhelp](https://redirect.github.com/renovatebot/renovate/discussions) if\nthat's undesired.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0Mi45Mi4xIiwidXBkYXRlZEluVmVyIjoiNDIuOTIuMSIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\n---------\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: otelbot <197425009+otelbot@users.noreply.github.com>\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-01-27T17:02:49Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/e1c7a802b626d7c8a6061e9f1a3ced60ac9417eb"
        },
        "date": 1769584109829,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.03,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Utkarsh Umesan Pillai",
            "username": "utpilla",
            "email": "66651184+utpilla@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b1edd55a1fc81a7c05a3274650011e128da0b269",
          "message": "[otap-df-engine] Fix error kind (#1908)\n\n## Changes\n- Use the correct error kind",
          "timestamp": "2026-01-29T01:01:39Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b1edd55a1fc81a7c05a3274650011e128da0b269"
        },
        "date": 1769662303438,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.12,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "2d1f9b0bd4eefcc144e4a89c69729921df7c0be3",
          "message": "fix: Batches may differ by field order after unification (#1922)\n\n# Change Summary\n\nNote this is a band-aid to avoid larger changes, but it does solve a\nbunch of panics.\n\n- Project batches to the merged schema before coalescing (reorder the\nfields to be the same)\n\n## What issue does this PR close?\n\nRelated to: https://github.com/open-telemetry/otel-arrow/issues/1334.\n\n## How are these changes tested?\n\nNew unit tests for the coalescing.\n\n## Are there any user-facing changes?\n\nNo.\n\n---------\n\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>",
          "timestamp": "2026-01-30T00:26:59Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2d1f9b0bd4eefcc144e4a89c69729921df7c0be3"
        },
        "date": 1769747368975,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.02,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6ad291b19e1b329ce9441810ea2b4a41cd1085eb",
          "message": "Allow mixed local/shared pdata senders (#1919)\n\n# Change Summary\n\n- Allow local receivers/processors to use the generic message::Sender so\nmixed local/shared edges can share channels safely.\n- Introduce ChannelMode to centralize control-channel wiring and\nmetrics, reducing duplication across wrappers making the overall design\nless error-prone.\n- Add pipeline test for mixed local/shared receivers targeting the same\nexporter.\n  \n  ## What issue does this PR close?\n\n  NA\n  \n  ## How are these changes tested?\n\n See pipeline_tests.rs\n\n  ## Are there any user-facing changes?\n\n  No",
          "timestamp": "2026-01-30T03:15:37Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/6ad291b19e1b329ce9441810ea2b4a41cd1085eb"
        },
        "date": 1769833560966,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.03,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6ad291b19e1b329ce9441810ea2b4a41cd1085eb",
          "message": "Allow mixed local/shared pdata senders (#1919)\n\n# Change Summary\n\n- Allow local receivers/processors to use the generic message::Sender so\nmixed local/shared edges can share channels safely.\n- Introduce ChannelMode to centralize control-channel wiring and\nmetrics, reducing duplication across wrappers making the overall design\nless error-prone.\n- Add pipeline test for mixed local/shared receivers targeting the same\nexporter.\n  \n  ## What issue does this PR close?\n\n  NA\n  \n  ## How are these changes tested?\n\n See pipeline_tests.rs\n\n  ## Are there any user-facing changes?\n\n  No",
          "timestamp": "2026-01-30T03:15:37Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/6ad291b19e1b329ce9441810ea2b4a41cd1085eb"
        },
        "date": 1769914361353,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.01,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6ad291b19e1b329ce9441810ea2b4a41cd1085eb",
          "message": "Allow mixed local/shared pdata senders (#1919)\n\n# Change Summary\n\n- Allow local receivers/processors to use the generic message::Sender so\nmixed local/shared edges can share channels safely.\n- Introduce ChannelMode to centralize control-channel wiring and\nmetrics, reducing duplication across wrappers making the overall design\nless error-prone.\n- Add pipeline test for mixed local/shared receivers targeting the same\nexporter.\n  \n  ## What issue does this PR close?\n\n  NA\n  \n  ## How are these changes tested?\n\n See pipeline_tests.rs\n\n  ## Are there any user-facing changes?\n\n  No",
          "timestamp": "2026-01-30T03:15:37Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/6ad291b19e1b329ce9441810ea2b4a41cd1085eb"
        },
        "date": 1770000195138,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.04,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "843e8d6887f93cbca0d74586f368cacd81eade1e",
          "message": "Performance improvement for adding transport optimized encoding (#1927)\n\n# Change Summary\n\n- Optimizes the implementation of applying transport optimized encoding.\n- Renames `materialize_parent_id` bench to `transport_optimize` as this\nnow contains benchmarks that do both encoding & decoding\n\n**Benchmark summary:**\n\n| Benchmark | Size | Nulls | Before (µs) | After (µs) | Speedup |\nImprovement |\n\n|-----------|------|-------|-------------|------------|---------|-------------|\n| encode_transport_optimized_ids | 127 | No | 48.037 | 16.298 | 2.95x |\n66.1% faster |\n| encode_transport_optimized_ids | 127 | Yes | 47.768 | 18.446 | 2.59x |\n61.4% faster |\n| encode_transport_optimized_ids | 1536 | No | 518.36 | 98.955 | 5.24x |\n80.9% faster |\n| encode_transport_optimized_ids | 1536 | Yes | 520.94 | 107.01 | 4.87x\n| 79.5% faster |\n| encode_transport_optimized_ids | 8096 | No | 3418.3 | 508.92 | 6.72x |\n85.1% faster |\n| encode_transport_optimized_ids | 8096 | Yes | 3359.5 | 545.16 | 6.16x\n| 83.8% faster |\n\nNulls* column above signifies there were null rows in the attribute\nvalues column. Ordinarily we wouldn't encode attributes like this in\nOTAP because it we'd use the AttributeValuesType::Empty value in the\ntype column, but we handle it because it is valid arrow data since the\ncolumns are nullable.\n\n**Context:** \nwhen fixing #966 we added code to eagerly remove the transport optimized\nencoding from when transforming attributes, and noticed a significant\nregression in the performance benchmarks, especially on OTAP-ATTR-OTAP\nscenario because we do a round trip decode/encode of the transport\noptimized encoding.\n\n**Changes**\n\nThis PR specifically focuses on optimizing adding the transport\noptimized encoding for attributes, as this is where all the time was\nbeing spent. Adding this encoding involves sorting the attribute record\nbatch by type, key, value, then parent_id, and adding delta encoding to\nthe parent_id column for sequences where type, key and value are all\nequal to the previous row (unless value is null, or the type is Map or\nSlice).\n\nBefore this change, we were doing this sorting using arrow's\n`RowConverter`. We'd then do a second pass over the dataset to find\nsequences where type/key/value were equal, and apply the delta encoding\nto the parent_id column.\n\nAlthough using the `RowConverter` is sometimes [an efficient way to sort\nmultiple\ncolumns](https://arrow.apache.org/blog/2022/11/07/multi-column-sorts-in-arrow-rust-part-2/),\nit's notable that the `RowConverter` actually expands the dictionaries\nfor all the columns before it sorts (see\nhttps://github.com/apache/arrow-rs/issues/4811). This is extremely\nexpensive for us since most of our attribute columns are dictionary\nencoded.\n\nThis PR changes the implementation to sort the attributes record batch\ndirectly, starting by combining type & key together (using the sorted\ndictionary values from the keys column), then sorting this hybrid\ncolumn. It then partitions the type column to identify the attributes\nvalue column for this segment of the sorted result, and partitions the\nkey column to find segments of the value column to sort together. For\neach segment, it sorts it, appends it to a builder for the new values\ncolumn. It then partitions the sorted segment of values and for each\nsegment takes the parent_ids for the value segment, sorts them, adds\ndelta encoding, and appends these to a buffer containing the encoded\nparent IDs. Then it combines everything together and produces the\nresult.\n\nThe advantages of this approach are a) it's a lot faster and b) we build\nup enough state during the sorting that we don't need to do a second\npass over the `RecordBatch` to add delta encoding.\n\nThere are quite a few transformations that happen, and I tried to do\nthese as efficiently as possible. This means working with arrow's\nbuffers directly in many places, instead of always using immutable\n`Array`s and compute kernels, which reduces quite a lot the amount of\nallocations.\n\n**Future Work/Followups**\nThere are some code paths I didn't spent a lot of time optimizing:\n- If the parent_id is a u32 which may be dictionary encoded, we simply\ncast it to a primitive array and then cast it back into a dict when\nwe're done. I did some quick testing and figure this adds ~10% overhead.\n- If the value type is something that could be in a dictionary (string,\nint, bytes, & ser columns), but isn't dictionary encoded, or if the type\nis boolean, the way we build up the result column allocates many small\narrays. This could be improved\n- If the key column is not dictionary encoded. I didn't spend very much\ntime optimizing this.\n\nThere's also probably some methods that we were using before to encode\nthe ID column that I need to go back and delete\n\n## What issue does this PR close?\n\nRelated to #1853 \n\n## How are these changes tested?\n\nExisting unit tests plus new ones\n\n## Are there any user-facing changes?\n\nNo\n\n---------\n\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-02-02T23:55:12Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/843e8d6887f93cbca0d74586f368cacd81eade1e"
        },
        "date": 1770095996665,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.19,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Aaron Marten",
            "username": "AaronRM",
            "email": "AaronRM@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "af1e8e04e20c0b020bf6cd3d33eb8ccebb781314",
          "message": "feat: add Windows support for CI workflows and conditional compilation in metrics and exporter modules (#1939)\n\n# Change Summary\n\nEnable `cargo clippy` and `cargo fmt` on Windows for CI\n\n## What issue does this PR close?\n\n* Closes #1938\n\n## How are these changes tested?\n\n* Validated that clippy and fmt are clean on Windows\n\n## Are there any user-facing changes?\n\nNo\n\n---------\n\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>",
          "timestamp": "2026-02-04T00:37:13Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/af1e8e04e20c0b020bf6cd3d33eb8ccebb781314"
        },
        "date": 1770192671431,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.04,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.06,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "00600327d39aee678e2f63bc5cd7cf99be343977",
          "message": "Remove OTel logging SDK in favor of internal logging setup (#1936)\n\n# Change Summary\n\nRemoves the OTel logging SDK since we have otap-dataflow-internal\nlogging configurable in numerous ways. Updates OTel feature settings to\ndisable the OTel logging SDK from the build.\n\n## What issue does this PR close?\n\nRemoves `ProviderMode::OpenTelemetry`, the OTel logging SDK and its\nassociated configuration (service::telemetry::logs::processors::*).\n\nFixes #1576.\n\n## Are there any user-facing changes?\n\nYes.\n\n**Note: this removes the potential to use the OpenTelemetry tracing\nsupport via the opentelemetry tracing appender. However, we view tracing\ninstrumentation as having limited value until otap-dataflow is properly\ninstrumented for tracing. When this happens, we are likely to use an\ninternal tracing pipeline.**\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-02-04T23:21:04Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/00600327d39aee678e2f63bc5cd7cf99be343977"
        },
        "date": 1770291934459,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.68,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.74,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fbf156023d3c7c6483c94598aecacd64cf66f986",
          "message": "[WIP] Stabilize pdata concatenate (#1953)\n\n# Change Summary\n\nThis is part of #1926 phase 1 and re-implements the concatenate\nfunction. I've fixed a lot of bugs, improved performance over the\nexisting concatenate function quite a bit (mostly by simplifying\nimplementation) and took some steps to prepare for phase 2 which will\nhopefully improve the interface for this module.\n\nMajor changes:\n- Moved this to the transform module, I plan to treat concatenation,\nsplitting, and maybe also re-indexing as additional transformations on\nOtapArrowRecords similar to removing transport optimized encodings\n- Added an (in my opinion) better `record_batch!` macro that supports\ndictionaries\n- Many bugs fixed for schema selection (especially structs) and\nnullability\n- Bugs fixed for boundary conditions for dictionary cardinality\nselection\n- Turned a lot of potential panics into schema mismatch errors\n- Lots of additional unit tests\n\nThings I deferred:\n\n- Changing the interface to concatenate to be based on OtapBatchStore\ndeferred to phase 2 because we can't plug it into the existing code\neasily\n- Moving reindexing into the concatenate operation deferred to phase 2\nbecause we couldn't plug it into the existing code easily\n- Some performance improvements mentioned in TODOs\n\nBenchmark results:\n\n| Configuration | New Implementation | Old Implementation | Speedup |\n|---------------|-------------------|-------------------|---------|\n| 10 inputs, 5 points | 28.18 us | 101.03 us | **3.58x** |\n| 10 inputs, 50 points | 29.82 us | 110.47 us | **3.70x** |\n| 100 inputs, 5 points | 246.37 us | 951.29 us | **3.86x** |\n| 100 inputs, 50 points | 267.27 us | 1,020.0 us | **3.82x** |\n| 1000 inputs, 5 points | 4.47 ms | 16.62 ms | **3.72x** |\n| 1000 inputs, 50 points | 4.98 ms | 17.44 ms | **3.50x** |\n\n\n## What issue does this PR close?\n\nRelated to #1926 \n\n## How are these changes tested?\n\nMany unit tests\n\n## Are there any user-facing changes?\n\nNo.",
          "timestamp": "2026-02-06T00:08:32Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/fbf156023d3c7c6483c94598aecacd64cf66f986"
        },
        "date": 1770351292908,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.8,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Aaron Marten",
            "username": "AaronRM",
            "email": "AaronRM@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "82f71508f8e598e78853335cce82195e894894cd",
          "message": "feat(quiver): skip expired WAL entries during replay (#1984)\n\n# Change Summary\n\nDuring WAL replay, entries older than the configured `max_age` retention\nare now skipped rather than replayed into new segments. Without this\nfiltering, replaying expired WAL entries would effectively reset their\nage to zero, causing data to be retained longer than intended by the\nconfigured policy. The cutoff is computed once before the replay loop\nand compared against each entry's ingestion_time (with no assumption\nabout WAL ordering). Skipped entries advance the cursor so they won't be\nretried, and the expired_bundles counter is incremented so operators\nhave visibility into filtered data. When *all* replayed entries are\nexpired (nothing is replayed), the cursor is explicitly persisted to the\nsidecar to avoid redundant re-scanning on subsequent restarts.\n\n## What issue does this PR close?\n\n* Closes #1980\n\n## How are these changes tested?\n\nTwo new tests cover the mixed old/fresh filtering case and the\nall-expired edge case, the latter including a third engine reopen to\nverify cursor persistence.\n\n## Are there any user-facing changes?\n\nNo, this is an optimization to the WAL recovery behavior. No config or\nuser-facing changes.",
          "timestamp": "2026-02-07T00:29:34Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/82f71508f8e598e78853335cce82195e894894cd"
        },
        "date": 1770439214556,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Aaron Marten",
            "username": "AaronRM",
            "email": "AaronRM@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "82f71508f8e598e78853335cce82195e894894cd",
          "message": "feat(quiver): skip expired WAL entries during replay (#1984)\n\n# Change Summary\n\nDuring WAL replay, entries older than the configured `max_age` retention\nare now skipped rather than replayed into new segments. Without this\nfiltering, replaying expired WAL entries would effectively reset their\nage to zero, causing data to be retained longer than intended by the\nconfigured policy. The cutoff is computed once before the replay loop\nand compared against each entry's ingestion_time (with no assumption\nabout WAL ordering). Skipped entries advance the cursor so they won't be\nretried, and the expired_bundles counter is incremented so operators\nhave visibility into filtered data. When *all* replayed entries are\nexpired (nothing is replayed), the cursor is explicitly persisted to the\nsidecar to avoid redundant re-scanning on subsequent restarts.\n\n## What issue does this PR close?\n\n* Closes #1980\n\n## How are these changes tested?\n\nTwo new tests cover the mixed old/fresh filtering case and the\nall-expired edge case, the latter including a third engine reopen to\nverify cursor persistence.\n\n## Are there any user-facing changes?\n\nNo, this is an optimization to the WAL recovery behavior. No config or\nuser-facing changes.",
          "timestamp": "2026-02-07T00:29:34Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/82f71508f8e598e78853335cce82195e894894cd"
        },
        "date": 1770519541198,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Aaron Marten",
            "username": "AaronRM",
            "email": "AaronRM@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "82f71508f8e598e78853335cce82195e894894cd",
          "message": "feat(quiver): skip expired WAL entries during replay (#1984)\n\n# Change Summary\n\nDuring WAL replay, entries older than the configured `max_age` retention\nare now skipped rather than replayed into new segments. Without this\nfiltering, replaying expired WAL entries would effectively reset their\nage to zero, causing data to be retained longer than intended by the\nconfigured policy. The cutoff is computed once before the replay loop\nand compared against each entry's ingestion_time (with no assumption\nabout WAL ordering). Skipped entries advance the cursor so they won't be\nretried, and the expired_bundles counter is incremented so operators\nhave visibility into filtered data. When *all* replayed entries are\nexpired (nothing is replayed), the cursor is explicitly persisted to the\nsidecar to avoid redundant re-scanning on subsequent restarts.\n\n## What issue does this PR close?\n\n* Closes #1980\n\n## How are these changes tested?\n\nTwo new tests cover the mixed old/fresh filtering case and the\nall-expired edge case, the latter including a third engine reopen to\nverify cursor persistence.\n\n## Are there any user-facing changes?\n\nNo, this is an optimization to the WAL recovery behavior. No config or\nuser-facing changes.",
          "timestamp": "2026-02-07T00:29:34Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/82f71508f8e598e78853335cce82195e894894cd"
        },
        "date": 1770605093982,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.91,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "5cdec9ce4d886ed0e53c07457fe3f5e29b6e66c6",
          "message": "InternalLogs - catch more scenarios of direct use of tracing (#2006)\n\nFollow up to\nhttps://github.com/open-telemetry/otel-arrow/pull/1987/changes#diff-01748cfa22e108f927f1500697086488ddb8d06bcd3e66db97f7b4cbc6927678",
          "timestamp": "2026-02-10T01:22:00Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5cdec9ce4d886ed0e53c07457fe3f5e29b6e66c6"
        },
        "date": 1770700044178,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0d0af9a8664649f5c330cdcb2becf5bd611ca404",
          "message": "Add support for schema key aliases in query engine Parsers (#1725)\n\nDraft PR to open discussion - The current `otlp-bridge` for the\n`recordset` engine uses the OpenTelemetry [log data model\nspec](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md)\nfor its initial schema keys (`Attributes`, `Timestamp`,\n`ObservedTimestamp`, `SeverityText`, etc).\n\nHowever, many well-versed in the OpenTelemetry space may be more used to\nthe snake case representation (`attributes`, `time_unix_nano`,\n`observed_time_unix_nano`, `severity_text`, etc) from the\n[proto](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/pdata/src/views/otlp/proto/logs.rs)\nrepresentation.\n\nDo we have any significant risks if we plan to support both? Inspired by\n`severity_text` reference in #1722, been on the back of my mind for a\nwhile.\n\nThis is still somewhat incomplete, could need more wiring for\nuser-provided aliases in bridge, but for the moment just doing it for\nknown OpenTelemetry fields.",
          "timestamp": "2026-02-10T23:42:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/0d0af9a8664649f5c330cdcb2becf5bd611ca404"
        },
        "date": 1770783355819,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.99,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "70c62ad23a1d932f7e95bf93f57d4c86c82927c3",
          "message": "Emit warning and skip unconnected nodes during engine build (#2023)\n\n# Change Summary\n\nAdd a pre-processing step at the start of pipeline build that gracefully\nremoves unconnected nodes from the incoming `PipelineConfig`.\n\nInput with unconnected nodes:\n```yaml\nnodes:\n  unconnected_receiver:\n    kind: receiver\n    plugin_urn: \"urn:otel:syslog_cef:receiver\"\n    out_ports:\n    config:\n      listening_addr: \"127.0.0.1:5514\"\n      protocol: tcp\n\n  connected_receiver:\n    kind: receiver\n    plugin_urn: \"urn:otel:syslog_cef:receiver\"\n    out_ports:\n      out_port:\n        destinations:\n          - connected_proc\n        dispatch_strategy: round_robin\n    config:\n      listening_addr: \"127.0.0.1:5514\"\n      protocol: tcp\n\n  unconnected_proc:\n    kind: processor\n    plugin_urn: \"urn:otel:batch:processor\"\n    out_ports:\n      out_port:\n        destinations:\n          - connected_proc\n        dispatch_strategy: round_robin\n    config:\n      otap:\n        min_size: 1\n        sizer: items\n      flush_timeout: 5s\n\n  connected_proc:\n    kind: processor\n    plugin_urn: \"urn:otel:debug:processor\"\n    out_ports:\n      out_port:\n        destinations:\n          - noop_exporter\n        dispatch_strategy: round_robin\n    config:\n      verbosity: detailed\n      mode: signal\n\n  noop_exporter:\n    kind: exporter\n    plugin_urn: \"urn:otel:noop:exporter\"  \n```\n\nOutput (confirmed that log was able to pass through remaining connected\nnodes with debug processor):\n```log\n2026-02-11T19:01:57.699Z  INFO  otap-df-engine::pipeline.build.unconnected_node.removed: Removed unconnected node from pipeline. [pipeline_group_id=default_pipeline_group, pipeline_id=default_pipeline, core_id=0, node_id=unconnected_receiver, node_kind=receiver] entity/pipeline.attrs: pipeline.id=default_pipeline pipeline.group.id=default_pipeline_group core.id=0 numa.node.id=0 process.instance.id=AGOE4FHDDZ7ZBMHGEFGLWWBHPE host.id=CPC-drewr-ZFPSN container.id=\n2026-02-11T19:01:57.706Z  INFO  otap-df-admin::endpoint.start: Admin HTTP server listening [bind_address=127.0.0.1:8080]\n2026-02-11T19:01:57.701Z  INFO  otap-df-engine::pipeline.build.unconnected_node.removed: Removed unconnected node from pipeline. [pipeline_group_id=default_pipeline_group, pipeline_id=default_pipeline, core_id=0, node_id=unconnected_proc, node_kind=processor] entity/pipeline.attrs: pipeline.id=default_pipeline pipeline.group.id=default_pipeline_group core.id=0 numa.node.id=0 process.instance.id=AGOE4FHDDZ7ZBMHGEFGLWWBHPE host.id=CPC-drewr-ZFPSN container.id=\n2026-02-11T19:01:57.702Z  WARN  otap-df-engine::pipeline.build.unconnected_nodes: Some pipeline nodes were removed because they had no active incoming or outgoing edges. These nodes will not participate in data processing. Check pipeline configuration if this is unintentional. [pipeline_group_id=default_pipeline_group, pipeline_id=default_pipeline, core_id=0, removed_count=2] entity/pipeline.attrs: pipeline.id=default_pipeline pipeline.group.id=default_pipeline_group core.id=0 numa.node.id=0 process.instance.id=AGOE4FHDDZ7ZBMHGEFGLWWBHPE host.id=CPC-drewr-ZFPSN container.id=\n2026-02-11T19:01:57.725Z  INFO  otap-df-otap::receiver.start: Starting Syslog/CEF Receiver [protocol=Tcp, listening_addr=127.0.0.1:5514] entity/node.attrs: node.id=connected_receiver node.urn=urn:otel:syslog_cef:receiver node.type=receiver pipeline.id=default_pipeline pipeline.group.id=default_pipeline_group core.id=0 numa.node.id=0 process.instance.id=AGOE4FHDDZ7ZBMHGEFGLWWBHPE host.id=CPC-drewr-ZFPSN container.id=\n\nReceived 1 resource logs\nReceived 1 log records\nReceived 0 events\nLogRecord #0:\n   -> ObservedTimestamp: 1770836524675426978\n   -> Timestamp: 1770836524000000000\n   -> SeverityText: INFO\n   -> SeverityNumber: 9\n   -> Attributes:\n      -> syslog.facility: 16\n      -> syslog.severity: 6\n      -> syslog.host_name: securityhost\n      -> syslog.tag: myapp[1234]\n      -> syslog.app_name: myapp\n      -> syslog.process_id: 1234\n      -> syslog.content: User admin logged in from 10.0.0.1 successfully [test_id=234tg index=1]\n   -> Trace ID:\n   -> Span ID:\n   -> Flags: 0\n```\n\nInput with no connected nodes:\n```yaml\nnodes:\n  recv:\n    kind: receiver\n    plugin_urn: \"urn:test:a:receiver\"\n    config: {}\n  proc:\n    kind: processor\n    plugin_urn: \"urn:test:b:processor\"\n    out_ports:\n      out:\n        destinations: [exp]\n        dispatch_strategy: round_robin\n    config: {}\n  exp:\n    kind: exporter\n    plugin_urn: \"urn:test:c:exporter\"\n    config: {}\n```\n\nOutput:\n```log\n2026-02-11T19:00:02.759Z  INFO  otap-df-engine::pipeline.build.unconnected_node.removed: Removed unconnected node from pipeline. [pipeline_group_id=default_pipeline_group, pipeline_id=default_pipeline, core_id=0, node_id=proc, node_kind=processor]\n2026-02-11T19:00:02.759Z  INFO  otap-df-engine::pipeline.build.unconnected_node.removed: Removed unconnected node from pipeline. [pipeline_group_id=default_pipeline_group, pipeline_id=default_pipeline, core_id=0, node_id=recv, node_kind=receiver]\n2026-02-11T19:00:02.759Z  INFO  otap-df-engine::pipeline.build.unconnected_node.removed: Removed unconnected node from pipeline. [pipeline_group_id=default_pipeline_group, pipeline_id=default_pipeline, core_id=0, node_id=exp, node_kind=exporter]\n2026-02-11T19:00:02.759Z  WARN  otap-df-engine::pipeline.build.unconnected_nodes: Some pipeline nodes were removed because they had no active incoming or outgoing edges. These nodes will not participate in data processing. Check pipeline configuration if this is unintentional. [pipeline_group_id=default_pipeline_group, pipeline_id=default_pipeline, core_id=0, removed_count=3]\n2026-02-11T19:00:02.759Z  ERROR otap-df-state::state.observed_error: [observed_event=EngineEvent { key: DeployedPipelineKey { pipeline_group_id: \"default_pipeline_group\", pipeline_id: \"default_pipeline\", core_id: 0 }, node_id: None, node_kind: None, time: SystemTime { tv_sec: 1770836402, tv_nsec: 759880158 }, type: Error(RuntimeError(Pipeline { error_kind: \"EmptyPipeline\", message: \"Pipeline has no connected nodes after removing unconnected entries — check pipeline configuration\", source: None })), message: Some(\"Pipeline encountered a runtime error.\") }]\n2026-02-11T19:00:02.760Z  ERROR otap-df-state::state.report_failed: [error=InvalidTransition { phase: Starting, event: Error(RuntimeError(Pipeline { error_kind: \"EmptyPipeline\", message: \"Pipeline has no connected nodes after removing unconnected entries — check pipeline configuration\", source: None })), message: \"event not valid for current phase\" }]\n2026-02-11T19:00:02.760Z  INFO  otap-df-admin::endpoint.start: Admin HTTP server listening [bind_address=127.0.0.1:8080]\nPipeline failed to run: Pipeline runtime error: Pipeline has no connected nodes after removing unconnected entries — check pipeline configuration\n```\n\n\n## What issue does this PR close?\n\n* Closes #2012\n\n## How are these changes tested?\n\nUnit tests and local engine runs.\n\n## Are there any user-facing changes?\n\n1. Engine is now more flexible and does not crash with unconnected nodes\npresent in the config.\n2. Engine provides visible error if there are no nodes provided instead\nof starting up successfully.",
          "timestamp": "2026-02-11T23:33:54Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/70c62ad23a1d932f7e95bf93f57d4c86c82927c3"
        },
        "date": 1770882647582,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.94,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 69.99,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Aaron Marten",
            "username": "AaronRM",
            "email": "AaronRM@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c44f94aa7ac0bd300118e10038d53283e2134112",
          "message": "feat: durable event names to quiver logging (#1988)\n\n# Change Summary\n\nAll log/trace calls in the quiver crate now use crate-private\n`otel_info!`, `otel_warn!`, `otel_error!`, and `otel_debug!` macros that\nenforce a required event name as the first argument. This ensures every\nlog event has a stable, machine-readable OpenTelemetry Event name\nfollowing the `quiver.<component>.<action>` convention.\n\n## What issue does this PR close?\n\nn/a\n\n## How are these changes tested?\n\nMinimal unit test for the macros, ensured existing tests pass.\n\n## Are there any user-facing changes?\n\nNo",
          "timestamp": "2026-02-12T23:39:45Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c44f94aa7ac0bd300118e10038d53283e2134112"
        },
        "date": 1770951778188,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 81.95,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.06,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "cbc03d838832e2dedba932c899b95cdf95b07594",
          "message": "Dataflow Engine Pipeline configuration stabilization (#2031)\n\n# Change Summary\n\nSorry in advance, this is a fairly large PR, but it's for a good reason\nas it aims to stabilize our configuration model, which we discussed\nduring our SIG meetings.\n\n- Reworked node identity to use type: NodeUrn and removed the old\nkind/plugin_urn split.\n- Evolved NodeUrn from a type alias to a concrete parsed type\n(namespace, id, kind) with zero-cost part access and canonical URN\nreconstruction.\n- Moved URN normalization/parsing logic into the node_urn module and\ncleaned up obsolete URN plumbing.\n- Fully removed node-level out_ports wiring from NodeUserConfig.\n- Externalized graph wiring into top-level connections in\nPipelineConfig.\n- Simplified connection syntax:\n    - removed out_port field from connections\n    - default source output is implicit (default)\n    - multi-output selection stays explicit via from: node[\"output\"]\n- Standardized naming around output ports:\n    - config fields use outputs and default_output\n    - default output name is `default`\n    - outputs/default_output are optional for single-output nodes\n- Replaced connection fanout schema with policy-oriented schema:\n- policies.dispatch with one_of (default) and broadcast. I believe\n`one_of` better reflect the underlying implementation (was never really\na round robin strategy as the channel receivers were competing\ntogether).\n- broadcast is currently parsed but rejected for multi-destination edges\n(reserved for future support)\n    - single-destination edges treat dispatch as no-op\n- Refactored PipelineConfigBuilder API for readability in tests:\n- one_of(src, targets) and broadcast(src, targets) for default output\n- one_of_output(src, output, targets) and broadcast_output(...) for\nexplicit output\n    - added to(src, dst) and to_output(src, output, dst) aliases\n- Updated engine wiring internals and channel identity labeling to use\ndispatch policy terminology (one_of/broadcast) consistently.\n- Updated docs and examples to the new model:\n\n**To do: update the configuration of our continuous benchmarks.**\n  \n## What issue does this PR close?\n\n* Closes #1970 \n* Closes #1828\n* Closes #1829 \n\n## How are these changes tested?\n\nAll unit tests passed\n\n## Are there any user-facing changes?\n\nThe structure of the configuration files have changed.",
          "timestamp": "2026-02-13T15:01:21Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/cbc03d838832e2dedba932c899b95cdf95b07594"
        },
        "date": 1771042663807,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.22,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "cbc03d838832e2dedba932c899b95cdf95b07594",
          "message": "Dataflow Engine Pipeline configuration stabilization (#2031)\n\n# Change Summary\n\nSorry in advance, this is a fairly large PR, but it's for a good reason\nas it aims to stabilize our configuration model, which we discussed\nduring our SIG meetings.\n\n- Reworked node identity to use type: NodeUrn and removed the old\nkind/plugin_urn split.\n- Evolved NodeUrn from a type alias to a concrete parsed type\n(namespace, id, kind) with zero-cost part access and canonical URN\nreconstruction.\n- Moved URN normalization/parsing logic into the node_urn module and\ncleaned up obsolete URN plumbing.\n- Fully removed node-level out_ports wiring from NodeUserConfig.\n- Externalized graph wiring into top-level connections in\nPipelineConfig.\n- Simplified connection syntax:\n    - removed out_port field from connections\n    - default source output is implicit (default)\n    - multi-output selection stays explicit via from: node[\"output\"]\n- Standardized naming around output ports:\n    - config fields use outputs and default_output\n    - default output name is `default`\n    - outputs/default_output are optional for single-output nodes\n- Replaced connection fanout schema with policy-oriented schema:\n- policies.dispatch with one_of (default) and broadcast. I believe\n`one_of` better reflect the underlying implementation (was never really\na round robin strategy as the channel receivers were competing\ntogether).\n- broadcast is currently parsed but rejected for multi-destination edges\n(reserved for future support)\n    - single-destination edges treat dispatch as no-op\n- Refactored PipelineConfigBuilder API for readability in tests:\n- one_of(src, targets) and broadcast(src, targets) for default output\n- one_of_output(src, output, targets) and broadcast_output(...) for\nexplicit output\n    - added to(src, dst) and to_output(src, output, dst) aliases\n- Updated engine wiring internals and channel identity labeling to use\ndispatch policy terminology (one_of/broadcast) consistently.\n- Updated docs and examples to the new model:\n\n**To do: update the configuration of our continuous benchmarks.**\n  \n## What issue does this PR close?\n\n* Closes #1970 \n* Closes #1828\n* Closes #1829 \n\n## How are these changes tested?\n\nAll unit tests passed\n\n## Are there any user-facing changes?\n\nThe structure of the configuration files have changed.",
          "timestamp": "2026-02-13T15:01:21Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/cbc03d838832e2dedba932c899b95cdf95b07594"
        },
        "date": 1771127885986,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "cbc03d838832e2dedba932c899b95cdf95b07594",
          "message": "Dataflow Engine Pipeline configuration stabilization (#2031)\n\n# Change Summary\n\nSorry in advance, this is a fairly large PR, but it's for a good reason\nas it aims to stabilize our configuration model, which we discussed\nduring our SIG meetings.\n\n- Reworked node identity to use type: NodeUrn and removed the old\nkind/plugin_urn split.\n- Evolved NodeUrn from a type alias to a concrete parsed type\n(namespace, id, kind) with zero-cost part access and canonical URN\nreconstruction.\n- Moved URN normalization/parsing logic into the node_urn module and\ncleaned up obsolete URN plumbing.\n- Fully removed node-level out_ports wiring from NodeUserConfig.\n- Externalized graph wiring into top-level connections in\nPipelineConfig.\n- Simplified connection syntax:\n    - removed out_port field from connections\n    - default source output is implicit (default)\n    - multi-output selection stays explicit via from: node[\"output\"]\n- Standardized naming around output ports:\n    - config fields use outputs and default_output\n    - default output name is `default`\n    - outputs/default_output are optional for single-output nodes\n- Replaced connection fanout schema with policy-oriented schema:\n- policies.dispatch with one_of (default) and broadcast. I believe\n`one_of` better reflect the underlying implementation (was never really\na round robin strategy as the channel receivers were competing\ntogether).\n- broadcast is currently parsed but rejected for multi-destination edges\n(reserved for future support)\n    - single-destination edges treat dispatch as no-op\n- Refactored PipelineConfigBuilder API for readability in tests:\n- one_of(src, targets) and broadcast(src, targets) for default output\n- one_of_output(src, output, targets) and broadcast_output(...) for\nexplicit output\n    - added to(src, dst) and to_output(src, output, dst) aliases\n- Updated engine wiring internals and channel identity labeling to use\ndispatch policy terminology (one_of/broadcast) consistently.\n- Updated docs and examples to the new model:\n\n**To do: update the configuration of our continuous benchmarks.**\n  \n## What issue does this PR close?\n\n* Closes #1970 \n* Closes #1828\n* Closes #1829 \n\n## How are these changes tested?\n\nAll unit tests passed\n\n## Are there any user-facing changes?\n\nThe structure of the configuration files have changed.",
          "timestamp": "2026-02-13T15:01:21Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/cbc03d838832e2dedba932c899b95cdf95b07594"
        },
        "date": 1771210244738,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.24,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8d843949245c99f14780fa2794e2bf5bdfb8983b",
          "message": "fix(deps): update module go.opentelemetry.io/collector/pdata to v1.51.0 (#2046)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n|\n[go.opentelemetry.io/collector/pdata](https://redirect.github.com/open-telemetry/opentelemetry-collector)\n| `v1.50.0` → `v1.51.0` |\n![age](https://developer.mend.io/api/mc/badges/age/go/go.opentelemetry.io%2fcollector%2fpdata/v1.51.0?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/go/go.opentelemetry.io%2fcollector%2fpdata/v1.50.0/v1.51.0?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>open-telemetry/opentelemetry-collector\n(go.opentelemetry.io/collector/pdata)</summary>\n\n###\n[`v1.51.0`](https://redirect.github.com/open-telemetry/opentelemetry-collector/blob/HEAD/CHANGELOG.md#v1510v01450)\n\n##### 💡 Enhancements 💡\n\n- `pkg/scraperhelper`: ScraperID has been added to the logs for metrics,\nlogs, and profiles\n([#&#8203;14461](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14461))\n\n##### 🧰 Bug fixes 🧰\n\n- `exporter/otlp_grpc`: Fix the OTLP exporter balancer to use\nround\\_robin by default, as intended.\n([#&#8203;14090](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14090))\n\n- `pkg/config/configoptional`: Fix `Unmarshal` methods not being called\nwhen config is wrapped inside `Optional`\n([#&#8203;14500](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14500))\nThis bug notably manifested in the fact that the\n`sending_queue::batch::sizer` config for exporters\nstopped defaulting to `sending_queue::sizer`, which sometimes caused the\nwrong units to be used\n  when configuring `sending_queue::batch::min_size` and `max_size`.\n\nAs part of the fix, `xconfmap` exposes a new\n`xconfmap.WithForceUnmarshaler` option, to be used in the `Unmarshal`\nmethods\nof wrapper types like `configoptional.Optional` to make sure the\n`Unmarshal` method of the inner type is called.\n\nThe default behavior remains that calling `conf.Unmarshal` on the\n`confmap.Conf` passed as argument to an `Unmarshal`\nmethod will skip any top-level `Unmarshal` methods to avoid infinite\nrecursion in standard use cases.\n\n- `pkg/confmap`: Fix an issue where configs could fail to decode when\nusing interpolated values in string fields.\n([#&#8203;14413](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14413))\nFor example, a header can be set via an environment variable to a string\nthat is parseable as a number, e.g. `1234`\n\n- `pkg/service`: Don't error on startup when process metrics are enabled\non unsupported OSes (e.g. AIX)\n([#&#8203;14307](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/14307))\n\n<!-- previous-version -->\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - \"before 8am on Monday\" (UTC),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My44LjUiLCJ1cGRhdGVkSW5WZXIiOiI0My44LjUiLCJ0YXJnZXRCcmFuY2giOiJtYWluIiwibGFiZWxzIjpbImRlcGVuZGVuY2llcyJdfQ==-->\n\n---------\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: otelbot <197425009+otelbot@users.noreply.github.com>\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-02-17T00:36:06Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8d843949245c99f14780fa2794e2bf5bdfb8983b"
        },
        "date": 1771295991476,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.23,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Andres Borja",
            "username": "andborja",
            "email": "76450334+andborja@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7962c78a3cd1bd64c8108288975f39780c072ccf",
          "message": "feat: Upgrade bytes library to resolve RUSTSEC-2026-0007 (#2055)\n\n# Change Summary\n\nResolves security vulnerability reported at\nhttps://rustsec.org/advisories/RUSTSEC-2026-0007.html\n\n## What issue does this PR close?\n\n\n* Closes #N/A\n\n## How are these changes tested?\n\nBuilding and running local tests\n\n## Are there any user-facing changes?\n\nNo",
          "timestamp": "2026-02-17T23:34:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7962c78a3cd1bd64c8108288975f39780c072ccf"
        },
        "date": 1771388773280,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "023d33a58414ed39e724d6892d3ceb68994b906c",
          "message": "[config] Move to `otel_dataflow/v1` config file structure, centralize ITS, and normalize policy resolution (#2056)\n\n# Change Summary\n\nThis PR completes the config model consolidation and policy refactor for\nthe dataflow engine.\n\n- Simplified configuration loading to a single root format:\nOtelDataflowSpec (version: otel_dataflow/v1).\n- Removed support for loading standalone PipelineConfig files as engine\nruntime input.\n  - Renamed top-level pipeline_groups to groups.\n- Moved internal telemetry pipeline (ITS) declaration from\npipeline-level fields (internal, internal_connections) to:\n      - engine.observability.pipeline.nodes\n      - engine.observability.pipeline.connections\n  - Consolidated engine settings into EngineConfig.\n- Removed pipeline ServiceConfig / duplicated telemetry config path;\ntelemetry is now configured centrally and via policy resolution.\n- Introduced/expanded hierarchical policy resolution (top-level -> group\n-> pipeline, with observability-specific override where applicable):\n      - policies.channel_capacity.control.node\n      - policies.channel_capacity.control.pipeline\n      - policies.channel_capacity.pdata (default set to 128)\n      - policies.health\n      - policies.telemetry\n- policies.resources.core_allocation (default top-level behavior: all\ncores)\n- Explicitly rejected resources policy on observability pipeline for\nnow.\n  - Unified resolved config handling:\n- OtelDataflowSpec::resolve() produces a deterministic resolved\nsnapshot.\n- Observability pipeline is represented in resolved pipelines with role\ntagging.\n  - Controller cleanup:\n- deduplicated run paths (run_forever / run_till_shutdown) through a\nshared execution path\n- consumes resolved config once and uses config-owned observability\ninternal IDs.\n- Updated docs/tests/config fixtures accordingly (including README.md\nand files under configs/).\n\n## What issue does this PR close?\n\n  - Closes #1833 \n  - Closes #1871 \n  - Partially #1830 \n  \n  ## How are these changes tested?\n\nChecked with `cargo xtask check` \n  \n## Are there any user-facing changes?\n\nYes (breaking config changes):\n\n- Runtime config must be OtelDataflowSpec with version:\notel_dataflow/v1.\n  - pipeline_groups is now groups.\n  - ITS config moved to engine.observability.pipeline.\n  - Pipeline-level service / telemetry path removed.\n- Policy fields moved/standardized under hierarchical policies sections.\n  \n## Example configuration file\n\n```yaml\nversion: otel_dataflow/v1\n\n# This configuration file reproduces the continuous benchmarking setup used\n# in our CI pipelines. The traffic generators, system under test, and backend\n# are all included in a single configuration for easier local testing/debugging.\n#\n# Runtime CLI overrides:\n# - --num-cores / --core-id-range override top-level\n#   `policies.resources.core_allocation`.\n# - Pipeline/group-level `policies.resources` still take precedence over that\n#   top-level value.\n# - --http-admin-bind overrides `engine.http_admin.bind_address`.\n#\n# If you want --num-cores / --core-id-range to drive all pipelines uniformly,\n# remove the pipeline-level `policies.resources` sections below.\n\n# Top-level policy.\n# Values below match the engine defaults (explicit to showcase the v1 policy model).\npolicies:\n  channel_capacity:\n    control:\n      node: 256\n      pipeline: 256\n    pdata: 128\n  health: {}\n  telemetry:\n    pipeline_metrics: true\n    tokio_metrics: true\n    channel_metrics: true\n  resources:\n    core_allocation:\n      type: all_cores\n\n# Engine-wide settings.\nengine:\n  http_admin:\n    bind_address: 127.0.0.1:8085\n  telemetry:\n    logs:\n      level: info\n\n  # Internal telemetry system (ITS) declaration.\n  observability:\n    pipeline:\n      nodes:\n        itr:\n          type: internal_telemetry:receiver\n          config: {}\n        sink:\n          type: noop:exporter\n          config: null\n      connections:\n        - from: itr\n          to: sink\n\n# Pipeline groups are used to logically separate sets of pipelines.\n# Resolution order for regular pipelines is:\n# pipeline.policies -> group.policies -> top-level policies\n# (replacement is per policy family, not deep-merge).\ngroups:\n  continuous_benchmark:\n    # Group-level policies are optional. This one is explicit and matches\n    # defaults, to demonstrate the hierarchy without changing behavior.\n    policies:\n      channel_capacity:\n        control:\n          node: 256\n          pipeline: 256\n        pdata: 128\n\n    pipelines:\n      # ======================================================================\n      # Traffic generation pipelines\n      # ======================================================================\n\n      # First traffic generator: static pre-generated dataset.\n      # Pipeline-level resources override group/top-level resources.\n      traffic_gen1:\n        policies:\n          resources:\n            core_allocation:\n              type: core_count\n              count: 15\n\n        nodes:\n          receiver:\n            type: traffic_generator:receiver\n            config:\n              data_source: static\n              generation_strategy: pre_generated\n              traffic_config:\n                signals_per_second: 150000\n                max_signal_count: null\n                metric_weight: 0\n                trace_weight: 0\n                log_weight: 30\n          exporter:\n            type: otlp:exporter\n            config:\n              grpc_endpoint: \"http://127.0.0.1:4327\"\n\n        connections:\n          - from: receiver\n            to: exporter\n\n      # Second traffic generator: dynamic generation from semantic conventions.\n      traffic_gen2:\n        policies:\n          resources:\n            core_allocation:\n              type: core_set\n              set:\n                - start: 21\n                  end: 35\n\n        nodes:\n          receiver:\n            type: traffic_generator:receiver\n            config:\n              traffic_config:\n                signals_per_second: 100000\n                max_signal_count: null\n                metric_weight: 0\n                trace_weight: 0\n                log_weight: 30\n              registry_path: https://github.com/open-telemetry/semantic-conventions.git[model]\n          exporter:\n            type: otlp:exporter\n            config:\n              grpc_endpoint: \"http://127.0.0.1:4337\"\n\n        connections:\n          - from: receiver\n            to: exporter\n\n      # ======================================================================\n      # System Under Test pipeline\n      # ======================================================================\n      sut:\n        policies:\n          resources:\n            core_allocation:\n              type: core_set\n              set:\n                - start: 0\n                  end: 1\n\n        nodes:\n          otlp_recv1:\n            type: otlp:receiver\n            config:\n              protocols:\n                grpc:\n                  listening_addr: \"127.0.0.1:4327\"\n                  wait_for_result: true\n          otlp_recv2:\n            type: otlp:receiver\n            config:\n              protocols:\n                grpc:\n                  listening_addr: \"127.0.0.1:4337\"\n                  wait_for_result: true\n\n          router:\n            type: type_router:processor\n            outputs: [\"logs\", \"metrics\", \"traces\"]\n            config: {}\n\n          retry:\n            type: retry:processor\n            config:\n              multiplier: 1.5\n\n          logs_exporter:\n            type: otlp:exporter\n            config:\n              grpc_endpoint: \"http://127.0.0.1:4328\"\n              max_in_flight: 6\n\n          metrics_exporter:\n            type: noop:exporter\n            config: null\n\n          spans_exporter:\n            type: noop:exporter\n            config: null\n\n        connections:\n          - from: otlp_recv1\n            to: router\n          - from: otlp_recv2\n            to: router\n          - from: router[\"logs\"]\n            to: retry\n          - from: router[\"metrics\"]\n            to: metrics_exporter\n          - from: router[\"traces\"]\n            to: spans_exporter\n          - from: retry\n            to: logs_exporter\n\n      # ======================================================================\n      # Backend pipeline\n      # ======================================================================\n      backend:\n        policies:\n          resources:\n            core_allocation:\n              type: core_set\n              set:\n                - start: 1\n                  end: 1\n\n        nodes:\n          receiver:\n            type: otlp:receiver\n            config:\n              protocols:\n                grpc:\n                  listening_addr: 127.0.0.1:4328\n\n          perf_noop:\n            type: noop:exporter\n            config: null\n\n        connections:\n          - from: receiver\n            to: perf_noop\n```\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-02-19T01:25:02Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/023d33a58414ed39e724d6892d3ceb68994b906c"
        },
        "date": 1771499186396,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fd92a9e19d4b95d0ad022c0132f4fb47dda71241",
          "message": "chore(deps): update dependency flask to v3.1.3 (#2076)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n| [Flask](https://redirect.github.com/pallets/flask)\n([changelog](https://flask.palletsprojects.com/page/changes/)) |\n`==3.1.2` → `==3.1.3` |\n![age](https://developer.mend.io/api/mc/badges/age/pypi/flask/3.1.3?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/pypi/flask/3.1.2/3.1.3?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>pallets/flask (Flask)</summary>\n\n###\n[`v3.1.3`](https://redirect.github.com/pallets/flask/releases/tag/3.1.3)\n\n[Compare\nSource](https://redirect.github.com/pallets/flask/compare/3.1.2...3.1.3)\n\nThis is the Flask 3.1.3 security fix release, which fixes a security\nissue but does not otherwise change behavior and should not result in\nbreaking changes compared to the latest feature release.\n\nPyPI: <https://pypi.org/project/Flask/3.1.3/>\nChanges: <https://flask.palletsprojects.com/page/changes/#version-3-1-3>\n\n- The session is marked as accessed for operations that only access the\nkeys but not the values, such as `in` and `len`.\n[GHSA-68rp-wp8r-4726](https://redirect.github.com/pallets/flask/security/advisories/GHSA-68rp-wp8r-4726)\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - \"before 8am every weekday\" (UTC),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4yNS4xMSIsInVwZGF0ZWRJblZlciI6IjQzLjI1LjExIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-02-20T01:11:54Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/fd92a9e19d4b95d0ad022c0132f4fb47dda71241"
        },
        "date": 1771576599238,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 82.37,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 70.37,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d747115a1de764f930503e015747a437aad3916b",
          "message": "Refactor otap/experimental to new contrib-nodes crate (#2084)\n\n# Change Summary\n\nFirst step towards the goal state mentioned in #1847. I chose to start\nwith the experimental subfolder as it has a limited number of nodes to\nconfirm methodology.\n\nI tried to also clean up the `Cargo.toml` dependencies in both\n`otap-df/otap` and `otap-df/contrib-nodes` to separate concerns, but it\nis always possible there are more unused that should be removed.\n\n## What issue does this PR close?\n\n* Progress towards #1847\n* Closes #2085 \n\n## How are these changes tested?\n\nUnit tests and ran `main.rs` with nodes from `contrib-nodes` enabled.\n\n## Are there any user-facing changes?\n\nNo, only developer-facing.",
          "timestamp": "2026-02-20T23:38:12Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d747115a1de764f930503e015747a437aad3916b"
        },
        "date": 1771648307817,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.26,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Drew Relmas",
            "username": "drewrelmas",
            "email": "drewrelmas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d747115a1de764f930503e015747a437aad3916b",
          "message": "Refactor otap/experimental to new contrib-nodes crate (#2084)\n\n# Change Summary\n\nFirst step towards the goal state mentioned in #1847. I chose to start\nwith the experimental subfolder as it has a limited number of nodes to\nconfirm methodology.\n\nI tried to also clean up the `Cargo.toml` dependencies in both\n`otap-df/otap` and `otap-df/contrib-nodes` to separate concerns, but it\nis always possible there are more unused that should be removed.\n\n## What issue does this PR close?\n\n* Progress towards #1847\n* Closes #2085 \n\n## How are these changes tested?\n\nUnit tests and ran `main.rs` with nodes from `contrib-nodes` enabled.\n\n## Are there any user-facing changes?\n\nNo, only developer-facing.",
          "timestamp": "2026-02-20T23:38:12Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d747115a1de764f930503e015747a437aad3916b"
        },
        "date": 1771728147175,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.26,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a4a96cf872b41fe44de4f25badc23db1aceeb575",
          "message": "Add endpoint override config options for OTLP HTTP exporter (#2089)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nsmall followup from\nhttps://github.com/open-telemetry/otel-arrow/pull/2070.\n\nAdds new config options for each signal type to override the endpoint to\nwhich the OTLP HTTP exporter sends data.\n\nThis is to aid with parity between this implementation and the analogous\nGo collector component, which also has these options:\nhttps://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter#otlp-http-exporter\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Part of #1145 \n\n## How are these changes tested?\n\nA new unit test is added.\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n \n Users can configure the component with these new options.",
          "timestamp": "2026-02-22T19:49:16Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a4a96cf872b41fe44de4f25badc23db1aceeb575"
        },
        "date": 1771819832526,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7aff0cf9268d4f9dbc8afaca22d13879aa07a0cf",
          "message": "Ability to validate config without engine start (#2065)\n\nOverrides https://github.com/open-telemetry/otel-arrow/pull/2057 and\nincludes validation of each node's config.\nAlso added a CI check to ensure all yaml configs in the repo is valid.\n(We had broken ones that was caught by this already and fixed in this\nPR)\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>\nCo-authored-by: Drew Relmas <drewrelmas@gmail.com>",
          "timestamp": "2026-02-23T23:57:56Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7aff0cf9268d4f9dbc8afaca22d13879aa07a0cf"
        },
        "date": 1771910896429,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.44,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Joshua MacDonald",
            "username": "jmacd",
            "email": "jmacd@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9737434e98d32d80ae6d020117bde5b7909f0840",
          "message": "Node telemetry_attributes into entity::extend::identity_attributes (#2101)\n\n# Change Summary\n\nMoves telemetry_attributes as requested.\n\n## What issue does this PR close?\n\nFixes #2078.\n\n## How are these changes tested?\n\n✅ \n\n## Are there any user-facing changes?\n\nYes, documented.\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-02-24T23:00:28Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9737434e98d32d80ae6d020117bde5b7909f0840"
        },
        "date": 1772003036097,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.46,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Pritish Nahar",
            "username": "pritishnahar95",
            "email": "pritishnahar@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "bffbd770fe67cfd3f3a8db6a5b6ba8cac82727bf",
          "message": "refactor: flip URN format from urn:ns:id:kind to urn:ns:kind:id (#2110)\n\n# Change Summary\n\nRefactor URN format: flip component name and type ordering from\n`urn:<namespace>:<id>:<kind>` to `urn:<namespace>:<kind>:<id>`.\n\nThe new ordering follows a general-to-specific hierarchy — you first\nspecify what kind of component it is (receiver, processor, exporter),\nthen which one. This makes URNs more intuitive to read and discover.\n\n**Before:**\n```\nurn:otel:otlp:receiver\nurn:otel:batch:processor\nurn:otel:noop:exporter\nurn:microsoft:geneva:exporter\n```\n\n**After:**\n```\nurn:otel:receiver:otlp\nurn:otel:processor:batch\nurn:otel:exporter:noop\nurn:microsoft:exporter:geneva\n```\n\nThe shortcut form is also updated from `<id>:<kind>` to `<kind>:<id>`\n(e.g., `receiver:otlp`).\n\nChanges span the following:\n- Core URN parsing logic (`node_urn.rs`): updated `parse()`,\n`build_node_urn()`, `split_segments()`, error messages, and all\ndoc-comments\n- All URN constant definitions across receiver, processor, and exporter\nimplementations\n- All YAML configuration files (quoted and unquoted URN values)\n- All test fixtures (JSON and YAML) and inline test strings\n- All documentation (`urns.md`, `configuration-model.md`,\n`otlp-receiver.md`, crate READMEs)\n- Added a test case verifying the old format is now rejected\n\n## What issue does this PR close?\n\n* Closes\n[#2108](https://github.com/open-telemetry/otel-arrow/issues/2108)\n\n## How are these changes tested?\n\n- All existing unit tests in `otap-df-config` pass with zero failures.\n- Updated test assertions to validate the new URN format.\n- Added explicit test case confirming old format\n(`urn:otel:otlp:receiver`) is rejected.\n- Full workspace build (`cargo check --workspace`) compiles cleanly.\n\n## Are there any user-facing changes?\n\nYes. All URN references in pipeline configuration files must use the new\n`urn:<namespace>:<kind>:<id>` format. The shortcut form changes from\n`<id>:<kind>` to `<kind>:<id>`. For example:\n- `urn:otel:otlp:receiver` → `urn:otel:receiver:otlp`\n- `otlp:receiver` → `receiver:otlp`\n\nExisting configurations using the old format will be rejected with a\nclear error message pointing to the URN documentation.\n\n---------\n\nCo-authored-by: Cijo Thomas <cijo.thomas@gmail.com>",
          "timestamp": "2026-02-25T23:47:47Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/bffbd770fe67cfd3f3a8db6a5b6ba8cac82727bf"
        },
        "date": 1772075612243,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.46,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb",
            "email": "lalit_fin@yahoo.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "274e9e1c0d2e09d205ef1a45aa5c327f3a6a3e9f",
          "message": "   [Geneva exporter] Add UserManagedIdentityByResourceId auth variant (#2096)\n\n### Description:\n\n- Adds `UserManagedIdentityByResourceId` variant to the Geneva\nexporter's `AuthConfig` enum, enabling authentication via Azure Resource\nManager resource ID. This maps to the existing\n`AuthMethod::UserManagedIdentityByResourceId` in `geneva-uploader` - no\nnew auth logic added.\n\n### Changes:\n- New `AuthConfig` variant with `resource_id` and `msi_resource` fields\n- Mapping to `AuthMethod::UserManagedIdentityByResourceId` in\n`from_config`\n   - `msi_resource` extraction for the new variant\n- Extended `test_auth_config_variants` to cover all five auth variants\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-02-27T00:29:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/274e9e1c0d2e09d205ef1a45aa5c327f3a6a3e9f"
        },
        "date": 1772160917933,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.47,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "532cc7dfa1a3ef24a3544b3af1419ccaeab71714",
          "message": "Columnar query engine expression evaluation: simple arithmetic (#2126)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nAdds a module to the columnar query engine with the ability evaluate\nsimple arithmetic expressions on OTAP record batches. For example, it\ncould evaluate expressions such as `severity_number + attributes[\"x\"] *\n2`.\n\nNote that the expression evaluation isn't yet integrated into any\n`PipelineStage` implementation, but the intention is that this can soon\nbe used to implement more advanced filtering, attribute insertion,\ncolumn updates and so on.\n\nWhile on the surface, this isolated simple arithmetic not appear\nterribly useful, the main/important contributions in this PR are to lay\ndown some foundations for future expression evaluation:\n\n**Transparent joins in the expression tree**\nThis PR adds is the ability to evaluate a set of DataFusion expressions\nwhile transparently joining data from different record batches as the\nexpression evaluates.\n\nFor example, consider we had `severity_number * 2 + attributes[\"x\"] +\nattributes[\"y\"]`, we'd need to first evaluate:\n- 1. `severity_number * 2`\n- 2. `attributes where key = \"x\"`, then select the value column based on\nthe type\n- 3. `attributes where key = \"y\"`, then select the value column based on\nthe type,\n\nThen we need to join these three expressions on the ID/parent ID\nrelationship, then perform the additions.\n\nThis PR builds the expression tree in such a way that it can manage\nwhere these joins need to happen on either side of a binary expression,\nand it performs the joins automatically during expression evaluation\nwhile keeping track of the ID scope/row order of the current data at\neach stage.\n\n**Type evaluation and coercion**\n\nWhile planning the expression, the planner attempts to keep track of\nwhat are the possible types that the expression could produce if it were\nto successfully evaluate. When it detects invalid types, it is able to\nproduce an error indicating an invalid expression.\n\nFor example, we'd be able to detect at planning time that `severity_text\n+ 2` is invalid, because text can't be added to a number.\n\nFor expressions where we can't determine that the types are invalid at\nplanning time, it will be determined and runtime and an error will be\nproduced when the expression evaluates on some batch. For example\n`attributes[\"x\"] + 2`, it's unknown whether `attributes[\"x\"]` is an\nint64, so it's assumed that the expression will produce an int64, and if\n`attributes[\"x\"]` is found not to be this type, an ExecutionError will\nbe produced.\n\nThe planner automatically coerces integer types when necessary.\nCurrently when adding two integers, they will be coerced into the\nlargest type that could contain the value, while keeping the signed-ness\nof one side. For example, uint8 + int32 will produce an int32. I realize\nthis type of automatic integer coercion is probably controversial, so in\nthe future I'm happy to get rid of this in favour of forcing explicit\ncasting if that is preferred.\n\n**Missing data / null propagation**\n\nWhen one side of an expression is null, for the purposes of arithmetic\nthe expression will evaluate to null. This includes the case of null\nvalues, missing attributes, missing columns, and missing optional record\nbatches.\n\nFor example: `attributes[\"x\"] + 2` would evaluate as null if the\nattribtues record batch was not present, there were no attributes with\n`key == \"x\"`, or the attributes where `key==\"x\"` had type empty, and so\non.\n\n**Relocated the projection code**\n\nAdds a new module called `pipeline::project` which has the projection\ncode that was previously inside the filter module. We need to project\nthe input record batches into a known schema to evaluate the\nexpressions, and also consider the expression evaluation to result in a\n`null` if the projection could evaluate due to missing data.\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Relates to https://github.com/open-telemetry/otel-arrow/issues/2058\n\n## How are these changes tested?\n\nThere are 64 new unit tests covering these changes\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\nNo\n\n\n## Future work/followups:\nThere are many, but most pressing are:\n- Other types of expression evaluation, including string expressions,\nunary math expressions, function invocation and bridging this with the\nfiltering code (for expressions that produce boolean arrays).\n- Integrating expression evaluation with various pipeline stages\nincluding those which set attributes, set values, and filtering\n- OPL Parser support for the type of expressions we're able to evaluate\n\n---------\n\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-02-28T00:22:00Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/532cc7dfa1a3ef24a3544b3af1419ccaeab71714"
        },
        "date": 1772245169646,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.55,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8b4f2c2f7f8ba2950f36749b7dced13ededee508",
          "message": "chore(deps): bump go.opentelemetry.io/otel/sdk from 1.39.0 to 1.40.0 in /collector/cmd/otelarrowcol (#2133)\n\nBumps\n[go.opentelemetry.io/otel/sdk](https://github.com/open-telemetry/opentelemetry-go)\nfrom 1.39.0 to 1.40.0.\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/blob/main/CHANGELOG.md\">go.opentelemetry.io/otel/sdk's\nchangelog</a>.</em></p>\n<blockquote>\n<h2>[1.40.0/0.62.0/0.16.0] 2026-02-02</h2>\n<h3>Added</h3>\n<ul>\n<li>Add <code>AlwaysRecord</code> sampler in\n<code>go.opentelemetry.io/otel/sdk/trace</code>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7724\">#7724</a>)</li>\n<li>Add <code>Enabled</code> method to all synchronous instrument\ninterfaces (<code>Float64Counter</code>,\n<code>Float64UpDownCounter</code>, <code>Float64Histogram</code>,\n<code>Float64Gauge</code>, <code>Int64Counter</code>,\n<code>Int64UpDownCounter</code>, <code>Int64Histogram</code>,\n<code>Int64Gauge</code>,) in\n<code>go.opentelemetry.io/otel/metric</code>.\nThis stabilizes the synchronous instrument enabled feature, allowing\nusers to check if an instrument will process measurements before\nperforming computationally expensive operations. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7763\">#7763</a>)</li>\n<li>Add <code>go.opentelemetry.io/otel/semconv/v1.39.0</code> package.\nThe package contains semantic conventions from the <code>v1.39.0</code>\nversion of the OpenTelemetry Semantic Conventions.\nSee the <a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/blob/main/semconv/v1.39.0/MIGRATION.md\">migration\ndocumentation</a> for information on how to upgrade from\n<code>go.opentelemetry.io/otel/semconv/v1.38.0.</code> (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7783\">#7783</a>,\n<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7789\">#7789</a>)</li>\n</ul>\n<h3>Changed</h3>\n<ul>\n<li>Improve the concurrent performance of\n<code>HistogramReservoir</code> in\n<code>go.opentelemetry.io/otel/sdk/metric/exemplar</code> by 4x. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7443\">#7443</a>)</li>\n<li>Improve the concurrent performance of\n<code>FixedSizeReservoir</code> in\n<code>go.opentelemetry.io/otel/sdk/metric/exemplar</code>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7447\">#7447</a>)</li>\n<li>Improve performance of concurrent histogram measurements in\n<code>go.opentelemetry.io/otel/sdk/metric</code>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7474\">#7474</a>)</li>\n<li>Improve performance of concurrent synchronous gauge measurements in\n<code>go.opentelemetry.io/otel/sdk/metric</code>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7478\">#7478</a>)</li>\n<li>Add experimental observability metrics in\n<code>go.opentelemetry.io/otel/exporters/stdout/stdoutmetric</code>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7492\">#7492</a>)</li>\n<li><code>Exporter</code> in\n<code>go.opentelemetry.io/otel/exporters/prometheus</code> ignores\nmetrics with the scope\n<code>go.opentelemetry.io/contrib/bridges/prometheus</code>.\nThis prevents scrape failures when the Prometheus exporter is\nmisconfigured to get data from the Prometheus bridge. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7688\">#7688</a>)</li>\n<li>Improve performance of concurrent exponential histogram measurements\nin <code>go.opentelemetry.io/otel/sdk/metric</code>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7702\">#7702</a>)</li>\n<li>The <code>rpc.grpc.status_code</code> attribute in the experimental\nmetrics emitted from\n<code>go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc</code>\nis replaced with the <code>rpc.response.status_code</code> attribute to\nalign with the semantic conventions. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7854\">#7854</a>)</li>\n<li>The <code>rpc.grpc.status_code</code> attribute in the experimental\nmetrics emitted from\n<code>go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc</code>\nis replaced with the <code>rpc.response.status_code</code> attribute to\nalign with the semantic conventions. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7854\">#7854</a>)</li>\n</ul>\n<h3>Fixed</h3>\n<ul>\n<li>Fix bad log message when key-value pairs are dropped because of key\nduplication in <code>go.opentelemetry.io/otel/sdk/log</code>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7662\">#7662</a>)</li>\n<li>Fix <code>DroppedAttributes</code> on <code>Record</code> in\n<code>go.opentelemetry.io/otel/sdk/log</code> to not count the\nnon-attribute key-value pairs dropped because of key duplication. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7662\">#7662</a>)</li>\n<li>Fix <code>SetAttributes</code> on <code>Record</code> in\n<code>go.opentelemetry.io/otel/sdk/log</code> to not log that attributes\nare dropped when they are actually not dropped. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7662\">#7662</a>)</li>\n<li>Fix missing <code>request.GetBody</code> in\n<code>go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp</code>\nto correctly handle HTTP/2 <code>GOAWAY</code> frame. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7794\">#7794</a>)</li>\n<li><code>WithHostID</code> detector in\n<code>go.opentelemetry.io/otel/sdk/resource</code> to use full path for\n<code>ioreg</code> command on Darwin (macOS). (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7818\">#7818</a>)</li>\n</ul>\n<h3>Deprecated</h3>\n<ul>\n<li>Deprecate <code>go.opentelemetry.io/otel/exporters/zipkin</code>.\nFor more information, see the <a\nhref=\"https://opentelemetry.io/blog/2025/deprecating-zipkin-exporters/\">OTel\nblog post deprecating the Zipkin exporter</a>. (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7670\">#7670</a>)</li>\n</ul>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/a3a5317c5caed1656fb5b301b66dfeb3c4c944e0\"><code>a3a5317</code></a>\nRelease v1.40.0 (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7859\">#7859</a>)</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/77785da545d67b38774891cbdd334368bfacdfd8\"><code>77785da</code></a>\nchore(deps): update github/codeql-action action to v4.32.1 (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7858\">#7858</a>)</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/56fa1c297bf71f0ada3dbf4574a45d0607812cc0\"><code>56fa1c2</code></a>\nchore(deps): update module github.com/clipperhouse/uax29/v2 to v2.5.0\n(<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7857\">#7857</a>)</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/298cbedf256b7a9ab3c21e41fc5e3e6d6e4e94aa\"><code>298cbed</code></a>\nUpgrade semconv use to v1.39.0 (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7854\">#7854</a>)</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/3264bf171b1e6cd70f6be4a483f2bcb84eda6ccf\"><code>3264bf1</code></a>\nrefactor: modernize code (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7850\">#7850</a>)</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/fd5d030c0aa8b5bfe786299047bc914b5714d642\"><code>fd5d030</code></a>\nchore(deps): update module github.com/grpc-ecosystem/grpc-gateway/v2 to\nv2.27...</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/8d3b4cb2501dec9f1c5373123e425f109c43b8d2\"><code>8d3b4cb</code></a>\nchore(deps): update actions/cache action to v5.0.3 (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7847\">#7847</a>)</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/91f7cadfcac363d67030f6913687c6dbbe086823\"><code>91f7cad</code></a>\nchore(deps): update github.com/timakin/bodyclose digest to 73d1f95 (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7845\">#7845</a>)</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/fdad1eb7f350ee1f5fdb3d9a0c6855cc88ee9d75\"><code>fdad1eb</code></a>\nchore(deps): update module github.com/grpc-ecosystem/grpc-gateway/v2 to\nv2.27...</li>\n<li><a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/commit/c46d3bac181ddaaa83286e9ccf2cd9f7705fd3d9\"><code>c46d3ba</code></a>\nchore(deps): update golang.org/x/telemetry digest to fcf36f6 (<a\nhref=\"https://redirect.github.com/open-telemetry/opentelemetry-go/issues/7843\">#7843</a>)</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/open-telemetry/opentelemetry-go/compare/v1.39.0...v1.40.0\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=go.opentelemetry.io/otel/sdk&package-manager=go_modules&previous-version=1.39.0&new-version=1.40.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\nYou can disable automated security fix PRs for this repo from the\n[Security Alerts\npage](https://github.com/open-telemetry/otel-arrow/network/alerts).\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-02-28T16:41:06Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8b4f2c2f7f8ba2950f36749b7dced13ededee508"
        },
        "date": 1772333341682,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.53,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "308023048a93dd306b0e1525808232b53afcdd7b",
          "message": "chore(deps): update docker digest updates (#2138)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| docker.io/rust | stage | digest | `4c7eb94` → `51c04d7` |\n| golang | stage | digest | `c83e68f` → `9edf713` |\n| python | final | digest | `9b81fe9` → `6a27522` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - \"before 8am on the first day of the\nmonth\" (UTC), Automerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n👻 **Immortal**: This PR will be recreated if closed unmerged. Get\n[config\nhelp](https://redirect.github.com/renovatebot/renovate/discussions) if\nthat's undesired.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My40My4yIiwidXBkYXRlZEluVmVyIjoiNDMuNDMuMiIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\n---------\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: otelbot <197425009+otelbot@users.noreply.github.com>",
          "timestamp": "2026-03-02T00:50:45Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/308023048a93dd306b0e1525808232b53afcdd7b"
        },
        "date": 1772419184678,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.53,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "63a23cf282c43a3dceb453f6fd17c794a4e9bb70",
          "message": "fix: Mark time_unix_nano as required for metrics histogram dp tables  (#2151)\n\n# Change Summary\n\nRemove `schema.Optional` metadata from histogram datapoint types.\n\n## What issue does this PR close?\n\n\n* Closes #2150\n\n## How are these changes tested?\n\nRan the unit tests\n\n## Are there any user-facing changes?\n\nNo\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-03-02T22:57:12Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/63a23cf282c43a3dceb453f6fd17c794a4e9bb70"
        },
        "date": 1772505671994,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.53,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Pritish Nahar",
            "username": "pritishnahar95",
            "email": "pritishnahar@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "00dae5b8f1d75035e7a920c38cc315f3a3dc1fcd",
          "message": "Upgrade Collector dependencies to v0.147.0/v1.53.0 (#2171)\n\n# Change Summary\nUpgrade Collector dependencies to v0.147.0.\nAs a part of this, upgrade `go.opentelemetry.io/otel/sdk` from v1.39.0\nto v1.40.0 in `collector/cmd/otelarrowcol` to remediate [CWE-426:\nUntrusted Search Path](https://cwe.mitre.org/data/definitions/426.html)\nSee: [GHSA vulnerability in go.opentelemetry.io/otel/sdk\nv1.20.0–v1.39.0](https://github.com/open-telemetry/opentelemetry-go/security/advisories)\n\n## What issue does this PR close?\nN/A\n\n## How are these changes tested?\n\nDependency-only change — no new application code. Verified via `go mod\ntidy` that the module graph resolves cleanly.\n\n## Are there any user-facing changes?\n\nNo.",
          "timestamp": "2026-03-04T00:35:48Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/00dae5b8f1d75035e7a920c38cc315f3a3dc1fcd"
        },
        "date": 1772591255438,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.58,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Copilot",
            "username": "Copilot",
            "email": "198982749+Copilot@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "4185b93a1656879195dab482af90287a8130984b",
          "message": "Fix --num-cores/--core-id-range CLI flags silently ignored when any policies block is present (#2154)\n\n`--num-cores` and `--core-id-range` were silently ignored whenever any\npipeline or group defined a `policies:` block — even for unrelated\nfields like `channel_capacity` — because `#[serde(default)]` on\n`Policies.resources: ResourcesPolicy` caused serde to materialize an\nimplicit `AllCores` default that the resolver treated as an explicit\ngroup-level override.\n\n# Change Summary\n\n## Root cause\n\n`resolve_resources_policy` used `.map(|p| p.resources.clone())` — which\nreturns `Some(ResourcesPolicy::default())` any time a `policies:` block\nexists, regardless of whether the user wrote `resources:`. This shadowed\nthe CLI override at the top-level config.\n\n## Changes\n\n- **`policy.rs`**: `Policies.resources` changed from `ResourcesPolicy` →\n`Option<ResourcesPolicy>` with `#[serde(default, skip_serializing_if =\n\"Option::is_none\")]`. Absent `resources:` now deserializes as `None`\ninstead of `AllCores`. Added `effective_resources() -> Cow<'_,\nResourcesPolicy>` for ergonomic access.\n\n- **`resolve.rs`**: `resolve_resources_policy` now uses `and_then` so a\n`None` resources at any scope falls through to the next, with\n`unwrap_or_default()` as the final fallback.\n\n- **`main.rs`**: `apply_cli_overrides` now sets\n`engine_cfg.policies.resources = Some(ResourcesPolicy { core_allocation\n})`.\n\n- **`controller/src/lib.rs`**: Core allocation access updated to\n`effective_resources().core_allocation`, extracted before partial moves\nof `channel_capacity`/`telemetry` fields to avoid borrow errors. Adds an\n`otel_info!` log per pipeline reporting both the resolved `num_cores`\ncount and the `core_allocation` strategy string (e.g. `*`, `[N cores]`,\nor a range-set), so startup logs confirm whether `--num-cores` actually\ntook effect. Test helper updated to use struct initializer form\n(`Policies { resources: Some(...), ..Default::default() }`) to satisfy\n`clippy::field_reassign_with_default`. Long method chain in core\nselection reformatted per `rustfmt` style.\n\n## What issue does this PR close?\n\n## How are these changes tested?\n\nAdded regression test\n`cli_num_cores_not_shadowed_by_implicit_default_resources` covering the\nexact scenario from the bug report: a group with `policies: {\nchannel_capacity: { pdata: 500 } }` and no explicit `resources:`,\ncombined with `--num-cores 4`. All existing controller tests continue to\npass. `cargo fmt` and `cargo clippy -D warnings` both pass.\n\n## Are there any user-facing changes?\n\nYes. Previously, `--num-cores`/`--core-id-range` were silently ignored\nwhen any `policies:` block existed at pipeline or group scope without an\nexplicit `resources:` key. After this fix, the CLI flag reliably takes\neffect as the global default unless a scope explicitly sets\n`resources.core_allocation` in YAML. Startup now logs both the resolved\ncore count and the core allocation strategy per pipeline via\n`otel_info!`, making it straightforward to confirm whether the CLI flag\nwas respected.\n\n<!-- START COPILOT ORIGINAL PROMPT -->\n\n\n\n<details>\n\n<summary>Original prompt</summary>\n\n> \n> ----\n> \n> *This section details on the original issue you should resolve*\n> \n> <issue_title>The --num-cores/--core-id-range CLI flags in df_engine\nare silently ignored when any pipeline or group defines a policies\nblock</issue_title>\n> <issue_description>The `--num-cores` and `--core-id-range` CLI flags\ndo not reliably control core allocation. When any pipeline or pipeline\ngroup defines a `policies:` block in YAML, even for an unrelated setting\nlike `channel_capacity`, the CLI core flags are silently ignored.\n> \n> ## Root Cause\n> \n> `apply_cli_overrides` in `src/main.rs` writes the CLI value into the\n**top-level** `engine_cfg.policies.resources.core_allocation`. Later,\n`resolve_resources_policy` in `crates/config/src/engine/resolve.rs`\nresolves the effective `ResourcesPolicy` for each pipeline using\nscope-level precedence (pipeline > group > top-level), returning the\n**entire** `ResourcesPolicy` struct from whichever scope first provides\na `policies` block.\n> \n> The problem is that `Policies` uses `#[serde(default)]` on all its\nfields. So if a user writes a `policies:` block at the pipeline or group\nlevel to configure *anything* (e.g., `channel_capacity`), the\ndeserializer also fills in a default `resources: { core_allocation:\nAllCores }` that the user never asked for. The resolver sees that this\nscope has a `ResourcesPolicy` and returns it, shadowing the CLI override\nat the top level.\n> \n> ## Example\n> \n> Given this config:\n> \n> ```yaml\n> groups:\n>   my_group:\n>     policies:\n>       channel_capacity:\n>         pdata: 500\n>     pipelines:\n>       my_pipeline:\n>         # ...\n> ```\n> Running `df_engine --config config.yaml --num-cores 4` will not use 4\ncores. The group-level policies: block exists (to set\n`channel_capacity`), so the deserializer fills in a default resources:\n`{ core_allocation: AllCores }`. Resolution picks that up instead of the\ntop-level value where the CLI wrote `CoreCount { count: 4 }`. The\npipeline runs on all cores with no warning.\n> \n> ## Workaround\n> \n> Set `core_allocation` explicitly at the pipeline or group level in\nYAML:\n> \n> ```yaml\n> groups:\n>   my_group:\n>     policies:\n>       channel_capacity:\n>         pdata: 500\n>       resources:\n>         core_allocation:\n>           type: core_count\n>           count: 4\n> ```\n> \n> ## Possible Fixes\n> \n> - Field-level merging: Resolve each policy field independently so that\nan unset resources at pipeline/group level falls through to top-level.\n> - Post-resolve CLI override: Apply --num-cores / --core-id-range to\nevery resolved pipeline's ResourcesPolicy after resolution, rather than\nonly to the pre-resolution top-level config.\n> - Warning: At minimum, detect and warn when a CLI core-allocation flag\nis effectively shadowed by an implicit default.\n> </issue_description>\n> \n> <agent_instructions>Use logs and confirm that the num-cores is\nreceived and respected properly.</agent_instructions>\n> \n> ## Comments on the Issue (you are @copilot in this section)\n> \n> <comments>\n> </comments>\n> \n\n\n</details>\n\n\n\n<!-- START COPILOT CODING AGENT SUFFIX -->\n\n- Fixes open-telemetry/otel-arrow#2098\n\n<!-- START COPILOT CODING AGENT TIPS -->\n---\n\n✨ Let Copilot coding agent [set things up for\nyou](https://github.com/open-telemetry/otel-arrow/issues/new?title=✨+Set+up+Copilot+instructions&body=Configure%20instructions%20for%20this%20repository%20as%20documented%20in%20%5BBest%20practices%20for%20Copilot%20coding%20agent%20in%20your%20repository%5D%28https://gh.io/copilot-coding-agent-tips%29%2E%0A%0A%3COnboard%20this%20repo%3E&assignees=copilot)\n— coding agent works faster and does higher quality work when set up for\nyour repo.\n\n---------\n\nCo-authored-by: copilot-swe-agent[bot] <198982749+Copilot@users.noreply.github.com>\nCo-authored-by: cijothomas <5232798+cijothomas@users.noreply.github.com>\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>\nCo-authored-by: lalitb <1196320+lalitb@users.noreply.github.com>\nCo-authored-by: Cijo Thomas <cijo.thomas@gmail.com>\nCo-authored-by: jmacd <3629705+jmacd@users.noreply.github.com>\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>\nCo-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-03-04T23:56:46Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/4185b93a1656879195dab482af90287a8130984b"
        },
        "date": 1772678298177,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.87,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "94df6e2702ae455f7e75d7983181cf0947a14485",
          "message": "fix: CI test failures (#2197)\n\n## Fix flaky `test_otap_receiver` test\n\nThe `test_otap_receiver` test was intermittently failing in CI with\n`\"Server did not shutdown\"`.\n\n### Root cause\n\nAfter sending a `Shutdown` control message, the test immediately\nattempted to connect to the gRPC server and asserted the connection must\nfail. Since `send_shutdown` only enqueues the message on a channel and\nreturns immediately, the server may not have closed the listener socket\nyet — a race condition that surfaces on slower/loaded CI runners.\n\n### Fix\n\nRemove the racy post-shutdown connection assertion. The shutdown\nbehavior is already validated by the test harness itself (the receiver\ntask completing cleanly). This is consistent with the\n`test_otap_receiver_ack` and `test_otap_receiver_nack` tests, which\ndon't perform this check.",
          "timestamp": "2026-03-05T18:09:17Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/94df6e2702ae455f7e75d7983181cf0947a14485"
        },
        "date": 1772764603262,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 87.03,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ed236e83c6a0e2c754ae40f1fcfdb012b75ee633",
          "message": "[query-engine] Replace drain calls with into_iter for consumed vectors (#2229)\n\n# Changes\n\n* Replace `drain(..)` calls with `into_iter()` where the source `Vec` is\nbeing consumed for better perf.",
          "timestamp": "2026-03-06T23:36:09Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ed236e83c6a0e2c754ae40f1fcfdb012b75ee633"
        },
        "date": 1772850191805,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 87.02,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ed236e83c6a0e2c754ae40f1fcfdb012b75ee633",
          "message": "[query-engine] Replace drain calls with into_iter for consumed vectors (#2229)\n\n# Changes\n\n* Replace `drain(..)` calls with `into_iter()` where the source `Vec` is\nbeing consumed for better perf.",
          "timestamp": "2026-03-06T23:36:09Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ed236e83c6a0e2c754ae40f1fcfdb012b75ee633"
        },
        "date": 1772937637441,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 87.02,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fc73f05c7c8cc416e57dab4232202987ef4f6c2f",
          "message": "Nit comment to RUST_LOG doc (#2231)\n\nhttps://github.com/open-telemetry/otel-arrow/pull/2146 following up with\nnits.",
          "timestamp": "2026-03-08T21:33:22Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/fc73f05c7c8cc416e57dab4232202987ef4f6c2f"
        },
        "date": 1773024098938,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 87.02,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "594347a94aaff0d805ccce98563849c12fb57117",
          "message": "Embedded o11y UI (#2234)\n\n# Change Summary\n\nThis PR replaces the previous admin dashboard with a richer\nobservability UI. This UI is integrated with the following endpoints:\n`/metrics`, `/readyz`, `/livez`, and `/status`.\n\nKey changes:\n- Integrates the new UI into otap-df-admin and embeds assets into the\nbinary.\n- Serves UI from:\n   - GET /\n   - GET /dashboard (alias)\n   - GET /static/* for embedded assets.\n- Vendors frontend runtime dependencies (i.e. tailwind and chartjs)\nunder crates/admin/ui/vendor (no CDN dependency at runtime).\n- Removes the old dashboard.html flow.\n\n## How are these changes tested?\n\n- cargo check -p otap-df-admin\n- scripts/run-ui-js-tests.sh\n\n## Are there any user-facing changes?\n\nYes, the UI has undergone many changes.\n\n---------\n\nCo-authored-by: Drew Relmas <drewrelmas@gmail.com>",
          "timestamp": "2026-03-09T18:05:43Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/594347a94aaff0d805ccce98563849c12fb57117"
        },
        "date": 1773109605570,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 88.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tom Tan",
            "username": "ThomsonTan",
            "email": "Tom.Tan@microsoft.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "14a5cf85771d517324335b39b428094acd898356",
          "message": "fix: correct label casing in issue templates to match existing repo labels (#2264)\n\n# Change Summary\n\nThe `bug_report.yaml` and `feature_request.yaml` issue templates had\nlabels\nthat didn't match any existing repository labels, so they were silently\nignored\nwhen issues were filed. This PR corrects them.\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Closes #2263\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->",
          "timestamp": "2026-03-11T00:49:57Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/14a5cf85771d517324335b39b428094acd898356"
        },
        "date": 1773195980499,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 88.18,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ee7dcbab5c0dec5d5cb234062849e4c747b940c9",
          "message": "CI flakiness - fix some tests (#2274)\n\nFix flaky udp_telemetry_refused_when_downstream_closed test by setting\nmax_batch_size=1 to flush immediately instead of relying on a\ntiming-dependent interval tick.",
          "timestamp": "2026-03-11T20:15:21Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ee7dcbab5c0dec5d5cb234062849e4c747b940c9"
        },
        "date": 1773283161808,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 88.54,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "bde436efd7b985136eec3bd265d685d115f76ae8",
          "message": "Add batchprocessor to perf tests (#2246)\n\nBlocked on https://github.com/open-telemetry/otel-arrow/issues/2194\n\nTrying to introduce batch processor to Perf tests, so as to catch ^\nissues earlier. And also to actually measure the perf impact of\nbatching!\n\n---------\n\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-03-13T00:21:19Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/bde436efd7b985136eec3bd265d685d115f76ae8"
        },
        "date": 1773369462173,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.58,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6933f8e2d5629a7b76983b60ff43526d67e7f33f",
          "message": "AzMonExporter - simplify auth retry and logging (#2311)\n\nRemoved redundant exponential backoff\nThe Azure SDK already performs exponential backoff internally (e.g., 6\nretries over 72s for IMDS via ManagedIdentityCredential). Our additional\nexponential backoff (5s → 30s with jitter) on top of that added\nnegligible value (4–30% extra wait) and unnecessary complexity. Replaced\nwith a fixed 1-second pause to prevent tight-spinning between SDK retry\ncycles.\n\n\nImproved get_token_failed WARN message\nAdded a message field that tells operators:\nToken acquisition failed\nThe exporter will keep retrying (counteracting the SDK's inner error\ntext which says \"the request will no longer be retried\")\nThe \"retries exhausted\" language in the error refers to an internal\nretry layer, not the exporter's outer loop\nFull error details remain available at DEBUG level via\nget_token_failed.details.\n\n\nBefore (two noisy WARN lines per failure, misleading retry timing):\n\n```txt\nWARN get_token_failed     [attempt=1, error=Auth error: ManagedIdentityCredential authentication failed. retry policy expired and the request will no longer be retried]\nWARN retry_scheduled      [delay_secs=5.23]\n```\n\nAfter (single clear WARN per failure, self-explanatory):\n\n```txt\nWARN get_token_failed     [message=Token acquisition failed. Will keep retrying. The error may mention retries being exhausted; that refers to an internal retry layer, not this outer loop., attempt=1, error=Auth error (token acquisition): ManagedIdentityCredential authentication failed. retry policy expired and the request will no longer be retried]\n```",
          "timestamp": "2026-03-13T23:05:22Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/6933f8e2d5629a7b76983b60ff43526d67e7f33f"
        },
        "date": 1773455262493,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.56,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6933f8e2d5629a7b76983b60ff43526d67e7f33f",
          "message": "AzMonExporter - simplify auth retry and logging (#2311)\n\nRemoved redundant exponential backoff\nThe Azure SDK already performs exponential backoff internally (e.g., 6\nretries over 72s for IMDS via ManagedIdentityCredential). Our additional\nexponential backoff (5s → 30s with jitter) on top of that added\nnegligible value (4–30% extra wait) and unnecessary complexity. Replaced\nwith a fixed 1-second pause to prevent tight-spinning between SDK retry\ncycles.\n\n\nImproved get_token_failed WARN message\nAdded a message field that tells operators:\nToken acquisition failed\nThe exporter will keep retrying (counteracting the SDK's inner error\ntext which says \"the request will no longer be retried\")\nThe \"retries exhausted\" language in the error refers to an internal\nretry layer, not the exporter's outer loop\nFull error details remain available at DEBUG level via\nget_token_failed.details.\n\n\nBefore (two noisy WARN lines per failure, misleading retry timing):\n\n```txt\nWARN get_token_failed     [attempt=1, error=Auth error: ManagedIdentityCredential authentication failed. retry policy expired and the request will no longer be retried]\nWARN retry_scheduled      [delay_secs=5.23]\n```\n\nAfter (single clear WARN per failure, self-explanatory):\n\n```txt\nWARN get_token_failed     [message=Token acquisition failed. Will keep retrying. The error may mention retries being exhausted; that refers to an internal retry layer, not this outer loop., attempt=1, error=Auth error (token acquisition): ManagedIdentityCredential authentication failed. retry policy expired and the request will no longer be retried]\n```",
          "timestamp": "2026-03-13T23:05:22Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/6933f8e2d5629a7b76983b60ff43526d67e7f33f"
        },
        "date": 1773543225570,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.59,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cijo.thomas@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6933f8e2d5629a7b76983b60ff43526d67e7f33f",
          "message": "AzMonExporter - simplify auth retry and logging (#2311)\n\nRemoved redundant exponential backoff\nThe Azure SDK already performs exponential backoff internally (e.g., 6\nretries over 72s for IMDS via ManagedIdentityCredential). Our additional\nexponential backoff (5s → 30s with jitter) on top of that added\nnegligible value (4–30% extra wait) and unnecessary complexity. Replaced\nwith a fixed 1-second pause to prevent tight-spinning between SDK retry\ncycles.\n\n\nImproved get_token_failed WARN message\nAdded a message field that tells operators:\nToken acquisition failed\nThe exporter will keep retrying (counteracting the SDK's inner error\ntext which says \"the request will no longer be retried\")\nThe \"retries exhausted\" language in the error refers to an internal\nretry layer, not the exporter's outer loop\nFull error details remain available at DEBUG level via\nget_token_failed.details.\n\n\nBefore (two noisy WARN lines per failure, misleading retry timing):\n\n```txt\nWARN get_token_failed     [attempt=1, error=Auth error: ManagedIdentityCredential authentication failed. retry policy expired and the request will no longer be retried]\nWARN retry_scheduled      [delay_secs=5.23]\n```\n\nAfter (single clear WARN per failure, self-explanatory):\n\n```txt\nWARN get_token_failed     [message=Token acquisition failed. Will keep retrying. The error may mention retries being exhausted; that refers to an internal retry layer, not this outer loop., attempt=1, error=Auth error (token acquisition): ManagedIdentityCredential authentication failed. retry policy expired and the request will no longer be retried]\n```",
          "timestamp": "2026-03-13T23:05:22Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/6933f8e2d5629a7b76983b60ff43526d67e7f33f"
        },
        "date": 1773629620680,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.59,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Trask Stalnaker",
            "username": "trask",
            "email": "trask.stalnaker@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "59ef72fdbe2003a1425bb5c700d3de0579ffb050",
          "message": "Migrate to new bare metal runner (Ubuntu 24) (#2338)\n\nOld runner:\n\n- name: `oracle-bare-metal-64cpu-512gb-x86-64`\n- 512gb memory\n- Oracle Linux 8\n\nNew runner:\n\n-  name: `oracle-bare-metal-64cpu-1024gb-x86-64-ubuntu-24`\n- 1024gb memory\n-  Ubuntu 24\n\nI realize this could have some impact on benchmark baselines, so please\npost on https://github.com/open-telemetry/community/issues/3333 once you\nhave migrated and are comfortable with the old one being removed.",
          "timestamp": "2026-03-16T22:28:38Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/59ef72fdbe2003a1425bb5c700d3de0579ffb050"
        },
        "date": 1773715315386,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.59,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "76e942858ac1aa0273636e1ca6888fecae444ce1",
          "message": "feat: Improve pdata::otap::testing module and expose it to other crates (#2349)\n\n# Change Summary\n\nThis is another PR on the path to #2289. During my initial\nimplementation, I realized that a lot of tests were generating invalid\notap batches. In order to make that easier, I've updated the\nlogs/metrics/traces macros in the pdata crate to automatically fill in\nanything required to be spec compliant based on what's been specified. I\nhad a half-baked version of this before, but now that we have the spec\nwe can do this properly.\n\nAdditional changes:\n\n- Added an `Into<OtapArrowRecords>` trait bound to OtapBatchStore.\nFrom<Logs/Metrics/Traces> was already implemented, it's just useful to\nhave in the bound.\n- added a `testing` feature which lets these be consumed across other\ncrates.\n\n## What issue does this PR close?\n\n* Part of #2289\n\n## How are these changes tested?\n\nUnit.\n\n## Are there any user-facing changes?\n\nNo\n\n---------\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-03-17T20:56:22Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/76e942858ac1aa0273636e1ca6888fecae444ce1"
        },
        "date": 1773801831360,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.7,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Gokhan Uslu",
            "username": "gouslu",
            "email": "geukhanuslu@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8ce6364e67702a4c46c76cf1e3fa0de89aa6e8a2",
          "message": "add resource id header to ame exporter requests (#2355)\n\n# Change Summary\n\nIf configured, sets resource id header for log analytics API.\n\n## What issue does this PR close?\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\nNew config field `azure_monitor_source_resourceid` under `api` config\nsection of azure monitor exporter.\n\n---------\n\nCo-authored-by: Drew Relmas <drewrelmas@gmail.com>",
          "timestamp": "2026-03-18T22:05:42Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8ce6364e67702a4c46c76cf1e3fa0de89aa6e8a2"
        },
        "date": 1773888306464,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 86.59,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9e27bdec52f01dfdd7a838c38b0484b0564f6102",
          "message": "feat: Enforce basic OTAP Spec compliance at the OtapBatchStore level (#2356)\n\n# Change Summary\n\nThis PR implements some basic spec enforcement as described in #2289. It\ndoesn't catch harder to detect things like duplicate primary ids, but it\ndoes account for making sure all fields match the spec and that there\nare no extraneous fields.\n\nMajority of this PR is various tweaks to different modules that depended\non setting invalid record batches being possible in tests or having the\noperation be infallible, but there are some major updates to be aware\nof:\n\n- I introduced the concept of RawBatchStore which now underpins\nOtapBatchStore. Validations are applied at the OtapBatchStore level and\nraw operations are generally unchecked and/or can panic. I think this is\nactually a good direction for slimming down the public contract for\nOtapBatchStore and there's probably more to explore here.\n- Updated the parquet exporter which was using OtapBatchStore as storage\nto use a new construct OtapParquetRecords which wraps RawBatchStore\ntypes becuase it needs to be able to do stuff like widen out the id\ncolumns\n- Split schema errors out into their own type\n- The `record_bundle` tests needed a little more reworking than was\ntypical elsewhere, I also consolidated a function into a generic there.\n\n## What issue does this PR close?\n\n* Closes #2289 \n\n## How are these changes tested?\n\nUnit.\n\n## Are there any user-facing changes?\n\nNo.\n\n---------\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-03-19T22:01:49Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9e27bdec52f01dfdd7a838c38b0484b0564f6102"
        },
        "date": 1773974336815,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 92.67,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c08121131bbc06a0c1712be05d5511d2d5b3e492",
          "message": "Columnar query engine `extend`/`set` operator support attiributes as destination (#2379)\n\nApologies to reviewers: I'm sorry this is long. I'm happy to break it up\nif desired.\n\n# Change Summary\n\nAdds the ability to set attribute values using the columnar query\nengine.\n\nNote that we already had this capability when the source was a static\nliteral (e.g. `logs | extend attributes[\"x\"] = \"hello\"`. This PR\nimproves the capability so the attribute can be assigned from the\nresults of the expression evaluation that was added in\nhttps://github.com/open-telemetry/otel-arrow/pull/2126\n\nIt means that now we can do things like:\n```kql\nlogs | set attributes[\"event_name\"] = event_name // use root field as source\nlogs | set attributes[\"x\"] = resource.attributes[\"y\"] // use some other attribute as the sourc\nlogs | set attributes[\"x\"] = attributes[\"y\"] * 2 // use arithmetic as the source\n// fyi ^^^ \"set\" is an alias of \"extend\" in OPL\n// etc.\n\n```\n\nNote: this does create Empty attributes if the expression evalutes to\n`null`. E.g. for something like this `logs | set attributes[\"x\"] =\nattributes[\"y\"]`, if `attributes[\"y\"]` did not exist for some row, then\nan empty attribute would be created for `attributes[\"x\"]`.\n\nThis also fixes a few bugs in the current set attributes implementation:\n- corrects the semantics of `set`/`extend` to be an \"upsert\" - e.g.\nreplace the attribute value or create a new attribute if one did not\nexist. Before this PR, we did not replace existing values.\n- fixes issue where attributes would not be inserted if the attribute\nrecord batch did not previously exist\n\nThe core of changes introduced is a optimized kernel for upserting\nattributes. Currently this lives is located\nat`otap_df_query_engine::pipeline::assign:attributes::upsert_attributes`.\nThis expects the caller to pass the attribute key, the new values, the\nparent_id associated with the attribute and a mask of which rows should\nbe updated. It then uses this to quickly merge the new values/attribute\ntypes, and append any inserts onto each column, inserting nulls where\nappropriate and maintaining correct dictionary encoding semantics. For\nbest performance, this can upsert multiple attribute keys at once.\n\nBecause the `upsert_attributes` kernel can assign multiple attributes at\nonce, the query-engine's planner code and the `AssignPipelineStage` have\nboth been modified to accomodate this. The planner attempts to coalesce\nmultiple \"set\" transformations into a single pipeline stage if possible,\nand the `AssignPipelineStage` handles evaluating the expression for each\nsource, and driving the invocation of the `upsert_attributes` kernel to\ndo all the attribute upserts in bulk.\n\n| Benchmark | 128 rows | 1536 rows | 8192 rows |\n|---|---|---|---|\n| `upsert_new_str_key` | 4.54 µs | 15.37 µs | 67.04 µs |\n| `upsert_existing_str_key` | 7.10 µs | 49.16 µs | 239.45 µs |\n| `upsert_two_new_str_keys` | 6.06 µs | 20.81 µs | 90.04 µs |\n| `upsert_two_existing_str_keys` | 9.50 µs | 58.26 µs | 271.99 µs |\n| `upsert_two_existing_one_new` | 11.41 µs | 65.20 µs | 300.78 µs |\n\nIn all cases, we see this is a lot faster than the current attribute\nupsert. (see here\nhttps://github.com/open-telemetry/otel-arrow/pull/2024). Although it's\nnot an apples-to-apples comparison, both these acheive the same result\non the same order-of-magnitude of log data, but this code is much\nfaster.\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Related to #2036 \n* Closes #2016\n\n## How are these changes tested?\n\nUnit tests - many new tests added\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\nYes - in transform processors users will now have the capability to\nassign the value of attributes using a variety of new expressions for\nsources (not just static literals).\n\n## Future work\n\nIn a future PR I'll integrate the new `upsert_attributes` kernel into\nthe attribute processor to improve performance and also fix #2350",
          "timestamp": "2026-03-20T20:16:16Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c08121131bbc06a0c1712be05d5511d2d5b3e492"
        },
        "date": 1774059973177,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 92.84,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d389678b03da242781069e748a409d90ffddf610",
          "message": "fix: Temporarily disable the nightly otap-filter-otap Go collector scenario (#2396)\n\n# Change Summary\n\nThis scenario has been blocking all the nightly benchmarks for a few\nweeks now and we can't fix it until this is released and we take a\nversion bump:\nhttps://github.com/open-telemetry/opentelemetry-collector-contrib/pull/46879\n\nIt looks like it will be another couple of weeks for the next otel\ncollector contrib release as the last one was just a few days ago. I'm\nproposing to disable the scenario for now to unblock everything else.",
          "timestamp": "2026-03-21T01:34:17Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d389678b03da242781069e748a409d90ffddf610"
        },
        "date": 1774147630531,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 92.84,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Google Antigravity",
            "username": "gyanranjanpanda",
            "email": "213113461+gyanranjanpanda@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "993e4a845f4a50d3cdbdbe074f40517bbe1741ee",
          "message": "feat: implement OtapMetricsView for zero-copy OTAP metrics traversal (#2367)\n\n## Summary\n\nImplement zero-copy OTAP Arrow-backed views for metrics data, following\nthe same pattern as OtapLogsView. This enables direct traversal of\nmetrics Arrow RecordBatches without intermediate conversion to protobuf\nor Prost types.\n\n## New file: views/otap/metrics.rs \n\nComplete metrics hierarchy:\n- OtapMetricsView → ResourceMetrics → ScopeMetrics → MetricView →\nDataView\n- Gauge/Sum/Histogram/ExpHistogram/Summary views\n- NumberDataPoint, HistogramDataPoint, ExpHistogramDataPoint,\nSummaryDataPoint views\n- ExemplarView, BucketsView, ValueAtQuantileView\n\n## Modified files (visibility only)\n- MetricsArrays/QuantileArrays/PositiveNegativeArrayAccess fields →\npub(crate)\n- Shared helpers in logs.rs → pub(crate) for reuse\n- views/otap.rs: added mod metrics + re-export\n\n## Design\n- Pre-computed BTreeMap indexes at construction (same as OtapLogsView)\n- Reuses RowGroup, OtapAttributeView, OtapAnyValueView from logs module\n- Introduces Otap32AttributeIter for u32-keyed dp/exemplar attributes\n\nCo-authored-by: Gyan Ranjan Panda <gyanranjanpanda@users.noreply.github.com>\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-03-22T14:45:01Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/993e4a845f4a50d3cdbdbe074f40517bbe1741ee"
        },
        "date": 1774234067128,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 92.84,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Gokhan Uslu",
            "username": "gouslu",
            "email": "geukhanuslu@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "84c4259bea0753ed03b05d19eba9c45a5a97d075",
          "message": "azure monitor exporter: gzip batcher compression ratio configurable + bench + default to 6 + fix surfaced bugs on edge cases (#2388)\n\n# Change Summary\n\n**NOTE:** In local end-to-end tests with a basic pipeline setup, higher\ncompression levels produced modestly higher throughput at the cost of\nmore CPU, because each batch carries more log records and fewer HTTP\nrequests are needed for the same data volume. This indicates the test\nsetup was I/O bound. Making the compression level configurable allows\ntuning the exporter based on whether the workload of a given pipeline\nconfiguration is I/O bound (favor higher levels) or CPU bound (favor\nlower levels).\n\n- Make gzip batcher compression level configurable via\n`gzip_compression_level` config field (0-9).\n- Add gzip batcher benchmarks across compression levels 1, 6, 9 and\nanalysis document.\n- Set default compression level to 6 (matching `flate2::GzEncoder`\ndefault).\n- Replace fixed `GZIP_SAFETY_MARGIN` with `TARGET_LIMIT` (1020 KiB) that\nleaves 4 KiB headroom below the 1 MiB API limit.\n- Fix `is_first_entry` to use `row_count == 0` instead of\n`uncompressed_size == 0`, which was incorrect after sync flushes.\n- Fix size accounting: structural JSON bytes (`[`, `,`, `]`) are now\nincluded in size tracking.\n- Remove unused `total_uncompressed_size` field.\n- Add utilization tests enforcing that waste stays under one entry's\nworth of `TARGET_LIMIT` across entry sizes (1B-64KB), data profiles\n(json_log/hex_json), and compression levels (1/6/9).\n- Add edge case tests: flush boundary comma validity, hard limit\nenforcement, cross-batch (spillover) JSON validity.\n- Add deterministic replay test (`test_replay_seed_89`) demonstrating\ngzip framing overhead with incompressible data near the limit boundary.\n- Refactor config tests to use `test_api_config()` helper to avoid churn\nwhen new fields are added.\n- Log `gzip_compression_level` at exporter startup.\n\n## Batch Utilization Results (TARGET_LIMIT = 1020 KiB)\n\n### Batch Sizes\n\n| Profile | Entry Size | Level 1 | Level 6 | Level 9 |\n| ---------- | ---------- | -------------------- | --------------------\n| -------------------- |\n| tiny_json | 1 B | 1,044,495 (100.00%) | 1,044,491 (100.00%) |\n1,044,492 (100.00%) |\n| hex_json | 10 B | 1,044,483 (100.00%) | 1,044,488 (100.00%) |\n1,044,482 (100.00%) |\n| json_log | 256 B | 1,044,394 (99.99%) | 1,044,283 (99.98%) | 1,044,378\n(99.99%) |\n| hex_json | 256 B | 1,044,282 (99.98%) | 1,044,378 (99.99%) | 1,044,293\n(99.98%) |\n| json_log | 1 KB | 1,043,561 (99.91%) | 1,043,712 (99.93%) | 1,043,708\n(99.93%) |\n| hex_json | 1 KB | 1,044,006 (99.95%) | 1,043,676 (99.92%) | 1,044,042\n(99.96%) |\n| json_log | 2 KB | 1,042,706 (99.83%) | 1,043,156 (99.87%) | 1,043,178\n(99.88%) |\n| json_log | 16 KB | 1,037,774 (99.36%) | 1,037,962 (99.38%) | 1,037,960\n(99.38%) |\n| hex_json | 16 KB | 1,034,652 (99.06%) | 1,028,748 (98.49%) | 1,028,745\n(98.49%) |\n| json_log | 64 KB | 997,315 (95.48%) | 1,017,014 (97.37%) | 1,016,850\n(97.35%) |\n| hex_json | 64 KB | 999,133 (95.66%) | 1,010,212 (96.72%) | 1,010,178\n(96.72%) |\n| mixed_json | 1B-16KB | 1,035,417 (99.13%) | 1,037,592 (99.34%) |\n1,037,693 (99.35%) |\n\nAll batches under 1 MiB (max observed: 1,044,495 bytes, 4,081 bytes\nbelow 1 MiB).\nUtilization relative to TARGET_LIMIT: 95.48%-100.00%. Waste never\nexceeds one entry's worth of TARGET_LIMIT.\n\n### Flush Counts\n\n| Profile    | Entry Size | Level 1 | Level 6 | Level 9 |\n| ---------- | ---------- | ------- | ------- | ------- |\n| tiny_json  | 1 B        | 21      | 30      | 31      |\n| hex_json   | 10 B       | 45      | 56      | 56      |\n| json_log   | 256 B      | 10      | 9       | 10      |\n| hex_json   | 256 B      | 11      | 11      | 11      |\n| json_log   | 1 KB       | 8       | 8       | 8       |\n| hex_json   | 1 KB       | 11      | 9       | 10      |\n| json_log   | 2 KB       | 7       | 7       | 7       |\n| json_log   | 16 KB      | 6       | 6       | 6       |\n| hex_json   | 16 KB      | 7       | 6       | 6       |\n| json_log   | 64 KB      | 4       | 4       | 4       |\n| hex_json   | 64 KB      | 5       | 5       | 5       |\n| mixed_json | 1B-16KB    | 6       | 6       | 6       |\n\nWorst case: 56 flushes (hex_json/10B at level 6/9), well under\nMAX_GZIP_FLUSH_COUNT = 100.\n\n## What issue does this PR close?\n\nN/A\n\n## How are these changes tested?\n\nBenchmarks and unit tests (29 gzip_batcher tests, 24 config tests).\n\n## Are there any user-facing changes?\n\nAdded a new optional config field `gzip_compression_level` (0-9, default\n6) for tuning compression level.",
          "timestamp": "2026-03-23T20:21:49Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/84c4259bea0753ed03b05d19eba9c45a5a97d075"
        },
        "date": 1774320028914,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 92.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0dde9c12802beebd157468f013b01a33cd40b3ac",
          "message": "feat: Log sampling processor (#2390)\n\n# Change Summary\n\nThis PR adds a very simple log sampling processor that's intended to be\nextensible and leave room for more sophisticated samplers in the future.\n\n## What issue does this PR close?\n\n* Closes #2382\n\n## How are these changes tested?\n\nUnit tests added - I also ran it locally and observed the metrics in the\ndashboard. This was with an `emit: 1, out_of: 10` and it dropped\nprecisely 90% according to the reported metrics.\n\n<img width=\"1745\" height=\"862\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/1faf1c68-b0fc-4193-9dd0-5de03e4399b3\"\n/>\n\nAnd the debug processor reported getting batches of exactly 10 which is\nexpected given traffic generator was spitting out batches of 100:\n\n<img width=\"407\" height=\"116\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/231c8e37-e93f-4567-b797-cab7223cc5f5\"\n/>\n\n\n## Are there any user-facing changes?\n\nYes, new processor!",
          "timestamp": "2026-03-24T19:10:26Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/0dde9c12802beebd157468f013b01a33cd40b3ac"
        },
        "date": 1774406682364,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9cdb34e73656de4317d0336c5c10c3da5c7ebda5",
          "message": "fix attributes dropped when decoding OTAP -> OTLP proto bytes when IDs are out of order (#2421)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nFixes issue decoding OTAP to OTLP proto bytes when the ID column of the\nroot record batch is not in sorted order.\n\nWhen we do this decoding, we initialize cursors for the order in which\nto visit the rows of each record batch. When we visit some row in the\nroot record batch, we then try to find its attributes by advancing the\nattributes cursor until the parent_id column matches the ID of the\ncurrent row in the root record batch.\n\nThe whole scheme is predicated on the assumption that we'll visit the\n`id`/`parent_id` column in the same order. However, we initialize the\ncursor for the attribtues record batch in sorted parent_id order, but\nfor the root record batch we were only initializing it in sorted order\nof resource/scope ID.\n\nThe fix is to add the ID column to the multi-column sort which is used\nto initialize the cursor for the root record batch.\n\nThis PR also makes the change to avoid using RowConverter for the\nmulti-column sort for the root record batch cursor init, instead opting\nto simply pack the IDs into a Vec of some unsized int (width depending\non how many ID columns we're packing), and then sort this. This slightly\nimproves performance (even after adding the additional column to the\nsort) and I added a benchmark to measure. Bench: main 337µs , after:\n305µs\n\nNote: one side-effect of this change (which in my opinion is OK), is\nthat all the rows with a null ID (e.g. rows w/ no attributes) will\nappear first in the decoded OTLP message. This is b/c arrow typically\nuses 0 as a placeholder in int arrays in null rows, and the sort we are\ndoing does not bother looking at the null buffer for best performance.\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Relates to #2270\n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n \n No",
          "timestamp": "2026-03-25T22:28:13Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9cdb34e73656de4317d0336c5c10c3da5c7ebda5"
        },
        "date": 1774493492867,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 92.98,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "5ef4ab53deb943ad109a7b7423c6f3566abd1bfa",
          "message": "[query-engine] Check schema when processing summaries back into logs in OTLP RecordSet bridge (#2441)\n\n# Changes\n\n* Check schema when converting summary records into log records in OTLP\nRecordSet bridge\n\n# Details\n\n@drewrelmas noticed if you do something like `\"source | summarize Count\n= count() by severity_text` or `\"source | summarize Count = count() |\nextend body = 'Summary record'` the data for `severity_text` or `body`\nends up on `Attributes`. This PR makes the summary code smarter to\ndetect when top-level things are set on a summary.",
          "timestamp": "2026-03-26T17:28:26Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5ef4ab53deb943ad109a7b7423c6f3566abd1bfa"
        },
        "date": 1774579983637,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 93.19,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sameer J",
            "username": "sjmsft",
            "email": "101909410+sjmsft@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "18fcc9e88e870a570bc5770d35e3ce80cb71a91a",
          "message": "Fix a grammatical error in the docs (#2448)\n\n# Change Summary\n\nFix a grammatical error in the README.md file.\n\n## What issue does this PR close?\n\nThis is a minor grammatical error being fixed to understand the workflow\nof creating and commit PRs in the otel-arrow project.\n\n## How are these changes tested?\n\nThis is a docs-only change, no testing required.\n\n## Are there any user-facing changes?\n\nMinor grammatical error is being fixed.",
          "timestamp": "2026-03-27T20:02:31Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/18fcc9e88e870a570bc5770d35e3ce80cb71a91a"
        },
        "date": 1774665852623,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 97.3,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sameer J",
            "username": "sjmsft",
            "email": "101909410+sjmsft@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "18fcc9e88e870a570bc5770d35e3ce80cb71a91a",
          "message": "Fix a grammatical error in the docs (#2448)\n\n# Change Summary\n\nFix a grammatical error in the README.md file.\n\n## What issue does this PR close?\n\nThis is a minor grammatical error being fixed to understand the workflow\nof creating and commit PRs in the otel-arrow project.\n\n## How are these changes tested?\n\nThis is a docs-only change, no testing required.\n\n## Are there any user-facing changes?\n\nMinor grammatical error is being fixed.",
          "timestamp": "2026-03-27T20:02:31Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/18fcc9e88e870a570bc5770d35e3ce80cb71a91a"
        },
        "date": 1774753009380,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 97.28,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sameer J",
            "username": "sjmsft",
            "email": "101909410+sjmsft@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "18fcc9e88e870a570bc5770d35e3ce80cb71a91a",
          "message": "Fix a grammatical error in the docs (#2448)\n\n# Change Summary\n\nFix a grammatical error in the README.md file.\n\n## What issue does this PR close?\n\nThis is a minor grammatical error being fixed to understand the workflow\nof creating and commit PRs in the otel-arrow project.\n\n## How are these changes tested?\n\nThis is a docs-only change, no testing required.\n\n## Are there any user-facing changes?\n\nMinor grammatical error is being fixed.",
          "timestamp": "2026-03-27T20:02:31Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/18fcc9e88e870a570bc5770d35e3ce80cb71a91a"
        },
        "date": 1774839560158,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 97.29,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Utkarsh Umesan Pillai",
            "username": "utpilla",
            "email": "66651184+utpilla@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "f8611f8124df885610809c3b99cc2e7434b1a0c2",
          "message": "Syslog CEF receiver: doc fixes and follow-up TODO from #2452 (#2461)\n\n# Change Summary\n\nSmall documentation improvements and a follow-up TODO for the syslog CEF\nreceiver, stemming from the review discussion on\nhttps://github.com/open-telemetry/otel-arrow/pull/2452#discussion_r3007878308.\n\n- TODO for message fragmentation: Add a TODO noting that truncated\nmessages currently emit the tail as a separate record without syslog\nheader context. Links to the review discussion for future work on\nfragment-correlation metadata.\n- Fix batching logic diagram: The ASCII diagram in README.md had\nmisaligned borders — the `max_batch_duration_ms` line was wider than the\nbox, causing the right-side `|` to not line up.\n- Add developer-reference note: Add a note at the top of\n`syslog-parsing-behavior.md` clarifying it is a developer/contributor\nreference doc and pointing to the README for user-facing configuration.\n\n## What issue does this PR close?\n\n* Closes #NNN\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\nNo\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <utpilla@users.noreply.github.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-03-31T00:15:18Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f8611f8124df885610809c3b99cc2e7434b1a0c2"
        },
        "date": 1774925650495,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 99.92,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Gokhan Uslu",
            "username": "gouslu",
            "email": "geukhanuslu@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d8f431884b821d2318d099a4aa587c1c75f5b6f9",
          "message": "Add heartbeat configuration to Azure Monitor Exporter (#2473)\n\n## Add heartbeat configuration to Azure Monitor Exporter\n\nAdds a `heartbeat` configuration section to the Azure Monitor Exporter,\nallowing operators to control heartbeat behavior and override\nauto-detected system fields.\n\nPart of #2475\n\n### Configuration\n\n```yaml\nheartbeat:\n  enabled: true       # default: false\n  frequency: 30       # seconds, default: 60\n  overrides:\n    version: \"2.0.0-custom\"\n    computer: \"my-host\"\n    os_type: \"Linux\"\n    os_name: \"Ubuntu\"\n    os_major_version: \"22\"\n    os_minor_version: \"04\"",
          "timestamp": "2026-03-31T22:42:07Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d8f431884b821d2318d099a4aa587c1c75f5b6f9"
        },
        "date": 1775012788083,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 100.28,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cijo Thomas",
            "username": "cijothomas",
            "email": "cithomas@microsoft.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fc1132647b423a2027e74729fec2d2b49594167c",
          "message": "Revert \"fix: Temporarily disable the nightly otap-filter-otap Go collector scenario\" (#2493)\n\nReverts open-telemetry/otel-arrow#2396 as we bumped to new Collector\nimage with the fix\nhttps://github.com/open-telemetry/otel-arrow/pull/2480",
          "timestamp": "2026-04-01T21:18:09Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/fc1132647b423a2027e74729fec2d2b49594167c"
        },
        "date": 1775098266019,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 100.56,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Ragu Marimuthu",
            "username": "ragumarimuthu-git",
            "email": "136855179+ragumarimuthu-git@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "854ffd851be5ef684e2a384de42d51c18bbea33d",
          "message": "Add OTLP forward config for gRPC (4315->4317) and HTTP (4316->4318) (#2524)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Closes #NNN\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n---------\n\nCo-authored-by: Ragu Marimuthu <136855179+Ragu2023@users.noreply.github.com>\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>",
          "timestamp": "2026-04-03T00:19:32Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/854ffd851be5ef684e2a384de42d51c18bbea33d"
        },
        "date": 1775184769180,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 100.66,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d8e64e0d5c26b3c31d2923336d8a59628f7b2bbf",
          "message": "feat(temporal_reaggregation_processor): Pass through metrics which cannot be aggregated (#2530)\n\n# Change Summary\n\nThis PR contains the necessary plumbing to construct a \"passthrough\"\nbatch and communicate the result of processing a view back up to the\nmain processing loop.\n\nBasically, for each batch we either:\n\n- Aggregate nothing and need to pass the data through as-is\n- Aggregate _some_ of the metrics and need to pass through what was not\naggregated instead of throwing it away\n- Aggregate all of the metrics in the batch\n\nTo support this we needed:\n\n- Exemplar support for the passthrough batch (still doesn't exist for\nthe aggregated batch)\n- A second set of state for building the passthrough batch\n\n## What issue does this PR close?\n\n* Part of #2422 \n\n## How are these changes tested?\n\nUnit.\n\n## Are there any user-facing changes?\n\nNo\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-04-04T00:48:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d8e64e0d5c26b3c31d2923336d8a59628f7b2bbf"
        },
        "date": 1775270613259,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 100.76,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d8e64e0d5c26b3c31d2923336d8a59628f7b2bbf",
          "message": "feat(temporal_reaggregation_processor): Pass through metrics which cannot be aggregated (#2530)\n\n# Change Summary\n\nThis PR contains the necessary plumbing to construct a \"passthrough\"\nbatch and communicate the result of processing a view back up to the\nmain processing loop.\n\nBasically, for each batch we either:\n\n- Aggregate nothing and need to pass the data through as-is\n- Aggregate _some_ of the metrics and need to pass through what was not\naggregated instead of throwing it away\n- Aggregate all of the metrics in the batch\n\nTo support this we needed:\n\n- Exemplar support for the passthrough batch (still doesn't exist for\nthe aggregated batch)\n- A second set of state for building the passthrough batch\n\n## What issue does this PR close?\n\n* Part of #2422 \n\n## How are these changes tested?\n\nUnit.\n\n## Are there any user-facing changes?\n\nNo\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-04-04T00:48:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d8e64e0d5c26b3c31d2923336d8a59628f7b2bbf"
        },
        "date": 1775358003587,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 100.76,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "d8e64e0d5c26b3c31d2923336d8a59628f7b2bbf",
          "message": "feat(temporal_reaggregation_processor): Pass through metrics which cannot be aggregated (#2530)\n\n# Change Summary\n\nThis PR contains the necessary plumbing to construct a \"passthrough\"\nbatch and communicate the result of processing a view back up to the\nmain processing loop.\n\nBasically, for each batch we either:\n\n- Aggregate nothing and need to pass the data through as-is\n- Aggregate _some_ of the metrics and need to pass through what was not\naggregated instead of throwing it away\n- Aggregate all of the metrics in the batch\n\nTo support this we needed:\n\n- Exemplar support for the passthrough batch (still doesn't exist for\nthe aggregated batch)\n- A second set of state for building the passthrough batch\n\n## What issue does this PR close?\n\n* Part of #2422 \n\n## How are these changes tested?\n\nUnit.\n\n## Are there any user-facing changes?\n\nNo\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-04-04T00:48:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d8e64e0d5c26b3c31d2923336d8a59628f7b2bbf"
        },
        "date": 1775444480557,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 100.76,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "c1ly",
            "username": "c1ly",
            "email": "129437996+c1ly@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "962ac30ac5ba928f035dbc513a34ed75787d83c4",
          "message": "[otap-dataflow] add transport header infrastructure (#2539)\n\n# Change Summary\n\nUpdate the OtapPdata Context with the transport_headers field, so that\nOtapPdata can carry the headers through the pipeline.\n\nDefined the TransportHeadersPolicy with HeaderCapturePolicy and\nHeaderPropagationPolicy, update the Policies to have a transport_headers\nfield. Receiver and Exporter nodes can also define HeaderCapturePolicy\nand HeaderPropagationPolicy respectively, these definitions will\noverride any top level HeaderCapturePolicy and HeaderPropagationPolicy\nrules.\n\nExposed the policy to the Receiver and Exporter nodes via the\nEffectHandler with helper functions that apply the policies on a\niterator of key value pairs (for receiver nodes) and transport_headers\n(for exporter nodes)\n\n## What issue does this PR close?\n\n* Closes #2508\n\n## How are these changes tested?\n\nunit tests and integration tests\n\n## Are there any user-facing changes?\n\nno",
          "timestamp": "2026-04-07T00:18:20Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/962ac30ac5ba928f035dbc513a34ed75787d83c4"
        },
        "date": 1775530563165,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 101,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jake Dern",
            "username": "JakeDern",
            "email": "33842784+JakeDern@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "72fba8d2a94cd5e20403875e9214756a86cd405f",
          "message": "feat(temporal_reaggregation_processor): Add processor microbenchmarks (#2571)\n\n# Change Summary\n\nThis is a PR to add some microbenchmarks for the\ntemporal_reaggregation_processor. This is nice vs using the e2e\nframework to isolate the processor and be able to measure improvement\nbeyond %cpu granularity.\n\n## What issue does this PR close?\n\n* Part of #2422",
          "timestamp": "2026-04-07T18:20:17Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/72fba8d2a94cd5e20403875e9214756a86cd405f"
        },
        "date": 1775616977945,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 101.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sameer J",
            "username": "sjmsft",
            "email": "101909410+sjmsft@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9b4b8dc2c00fb378ee2a7640a1b20189558f7b7f",
          "message": "fix: handle DrainIngress in fake_data_generator to unblock graceful shutdown (#2515)\n\n# Change Summary\n\nThe \"Ack nack redesign\" PR (3dca2837) introduced a two-phase\nDrainIngress/ReceiverDrained shutdown protocol but missed updating the\nfake_data_generator receiver. Without the DrainIngress handler, the\nmessage falls into the _ => {} catch-all, notify_receiver_drained() is\nnever called, the pipeline controller never removes the receiver from\nits pending set, and after the deadline expires it emits\nDrainDeadlineReached. This was causing pipeline-perf-test-basic to fail\nconsistently.\n\n## What issue does this PR close?\n\npipeline-perf-test-basic unit test is failing.\n\n* Closes #2511\n\n## How are these changes tested?\n\nfake_data_generator and runtime_control_metrics tests were executed.\n\n## Are there any user-facing changes?\n\nNo, fake_data_generator is an internal test/load-generation receiver,\nnot a user-facing component.\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>\nCo-authored-by: Joshua MacDonald <josh.macdonald@gmail.com>\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>",
          "timestamp": "2026-04-09T00:04:43Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9b4b8dc2c00fb378ee2a7640a1b20189558f7b7f"
        },
        "date": 1775703962210,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 101.98,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b5f0814099566c119a29aa8465a137e04adbeeb4",
          "message": "OPL/Columnar Query Engine Support some `TextExpression` variants (#2586)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nOPL / Columnar Query Engine support the `Concat`, `Join` and `Replace`\nvariants of the\n[`TextExpression`](https://github.com/open-telemetry/otel-arrow/blob/72fba8d2a94cd5e20403875e9214756a86cd405f/rust/experimental/query_engine/expressions/src/scalars/text_scalar_expression.rs#L7-L23).\nIn all these cases, we parse from specially named functions:\n```js\nlogs | set attributes[\"x\"] = concat(\"the\", \" attribute value \", \"is: \", attributes[\"x\"])\nlogs | set event_name = join(\" \", \"event happened:\", event_name)\nlogs | set event_name = replace(event_name, \"otel\", \"otap\")\n```\n\nIn each of these cases, we use the equivalent datafusion scalar function\n`concat`, `concat_ws` (for join) and `replace`.\n\nNote: `concat_ws` is also used as an alias for `join`. I was thinking\nthis'd be helpful for folks coming from a datafusion/SQL background. So\nit's equally possibly to write an expression like:\n```js\nlogs | set event_name = concat_ws(\" \", \"event happened:\", event_name)\n```\n\nIn `planner.rs`, I refactored the planning of function arguments into a\nreusable helper function.\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Related to #2578\n\n## How are these changes tested?\n\nUnit\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n \nThese new expression types are now supported via the transform\nprocessor.\n\n## Future work\n\nWill add support for the `TextExpression::Capture` variant of this\nexpression in future PR.",
          "timestamp": "2026-04-09T16:15:29Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b5f0814099566c119a29aa8465a137e04adbeeb4"
        },
        "date": 1775790242318,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 101.94,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b5f0814099566c119a29aa8465a137e04adbeeb4",
          "message": "OPL/Columnar Query Engine Support some `TextExpression` variants (#2586)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nOPL / Columnar Query Engine support the `Concat`, `Join` and `Replace`\nvariants of the\n[`TextExpression`](https://github.com/open-telemetry/otel-arrow/blob/72fba8d2a94cd5e20403875e9214756a86cd405f/rust/experimental/query_engine/expressions/src/scalars/text_scalar_expression.rs#L7-L23).\nIn all these cases, we parse from specially named functions:\n```js\nlogs | set attributes[\"x\"] = concat(\"the\", \" attribute value \", \"is: \", attributes[\"x\"])\nlogs | set event_name = join(\" \", \"event happened:\", event_name)\nlogs | set event_name = replace(event_name, \"otel\", \"otap\")\n```\n\nIn each of these cases, we use the equivalent datafusion scalar function\n`concat`, `concat_ws` (for join) and `replace`.\n\nNote: `concat_ws` is also used as an alias for `join`. I was thinking\nthis'd be helpful for folks coming from a datafusion/SQL background. So\nit's equally possibly to write an expression like:\n```js\nlogs | set event_name = concat_ws(\" \", \"event happened:\", event_name)\n```\n\nIn `planner.rs`, I refactored the planning of function arguments into a\nreusable helper function.\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Related to #2578\n\n## How are these changes tested?\n\nUnit\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n \nThese new expression types are now supported via the transform\nprocessor.\n\n## Future work\n\nWill add support for the `TextExpression::Capture` variant of this\nexpression in future PR.",
          "timestamp": "2026-04-09T16:15:29Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b5f0814099566c119a29aa8465a137e04adbeeb4"
        },
        "date": 1775875664672,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 101.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sameer J",
            "username": "sjmsft",
            "email": "101909410+sjmsft@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9f8e589b22501ac94dc6a1e89d54da10e2eeeb7e",
          "message": "refactor(engine): extract IndexedMinHeap from node-local wakeup scheduler (#2627)\n\n# Change Summary\n\nExtracts the hand-rolled binary heap (Vec<ScheduledWakeup> +\nHashMap<WakeupSlot, usize>) from NodeLocalScheduler into a generic,\nreusable IndexedMinHeap<K, P> data structure with its own module and\ntest suite.\n\n## What issue does this PR close?\n\n* Closes #2587 \n\n## How are these changes tested?\n\ncargo test -p otap-df-engine -- \"indexed_min_heap|node_local_scheduler\"\n\nExisting unit tests in node_local_scheduler and new unit tests in\nindexed_min_heap.\n\n## Are there any user-facing changes?\n\nNo. All changes are internal to otap-df-engine. No downstream crate or\nuser-facing behavior changes.",
          "timestamp": "2026-04-11T01:08:20Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9f8e589b22501ac94dc6a1e89d54da10e2eeeb7e"
        },
        "date": 1775963164307,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 101.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb",
            "email": "lalit_fin@yahoo.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "5868ff15d7307129796bf35ba0f322a08a8d3586",
          "message": " feat: Remove experimental-tls feature flag and make TLS always available  (#2624)\n\n### Summary                                 \n                                          \nRemove the `experimental-tls` feature gate and make TLS support\navailable by\ndefault across OTAP Dataflow.\n   \n### What changed\n                  \n- Removed the `experimental-tls` feature wiring from the workspace and\nall\n    affected crates\n  - Made the core TLS dependencies in `otap-df-otap` unconditional\n  - Removed feature-gated TLS fallback paths and the obsolete\n    `TlsFeatureDisabled` error variant    \n  - Made existing TLS tests compile and run by default\n- Updated configs, scripts, and docs to stop referring to\n`experimental-tls`\n- Added a binary-level compile-time guard in `df_engine` so normal\nbuilds must\nenable exactly one crypto provider:\n    - `crypto-ring`\n- `crypto-aws-lc`\n    - `crypto-openssl`                        \n  ### Notes       \n- This change does not alter the existing `crypto-*` feature flags; it\nonly\nremoves the compile-time gate around TLS availability.\n- `tonic/tls-native-roots` was intentionally not made unconditional.\nNative\ntrust anchors are loaded directly via `rustls_native_certs` in the TLS\nhelper\n    paths, so this is not an omission.",
          "timestamp": "2026-04-12T06:29:36Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5868ff15d7307129796bf35ba0f322a08a8d3586"
        },
        "date": 1776049918128,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 105.36,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Thomas",
            "username": "thperapp",
            "email": "88447796+thperapp@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ac4ee0b9fa2f3e68cea9c11a21c52372e50ad163",
          "message": "Fix typo in Prometheus test config filename (#2648)\n\n# Change Summary\nFix typo in test config filename\nfake-debug-noop-prometh'~~u~~'eus-telemetry.yaml ->\nfake-debug-noop-prometheus-telemetry.yaml\n\n## What issue does this PR close?\nminor nit\n\n## How are these changes tested?\nN/A\n\n## Are there any user-facing changes?\nN/A",
          "timestamp": "2026-04-13T23:38:17Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ac4ee0b9fa2f3e68cea9c11a21c52372e50ad163"
        },
        "date": 1776135896573,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 105.43,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "albertlockett",
            "username": "albertlockett",
            "email": "a.lockett@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "706037b6dd9d5707299b6f84ca31c1e6942a2069",
          "message": "Support function calls where args have differing row orders (#2635)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nAdds capability to the columnar query engine to invoke functions where\nthe columns passed as arguments have a different \"row order\"..\n\nConsider calling a function in OPL or KQL like `my_func(severity_text,\nattributes[\"x\"])`. In these cases, the `severity_text` would have the\n\"row order\" of the root record batch, where `attributes[\"x\"]` would have\nthe \"row order\" of however the `parent_id`s were sorted in the Log Attrs\nbatch rows where `key == \"x\"`.\n\n(We can think of \"row order\" here as order we'd encounter a row\nbelonging to some signal (e.g. log, span, etc.) represented as we scan\nthe column).\n\nCurrently the columnar query engine doesn't handle the case where\nfunction args have different \"row order\" and will just return an error.\nThis PR resolves the issue.\n\nIt does this by determining if the arguments to some function have\ndifferent \"row order\" (in the parlance of the engine's expressions\nplanning, we say they have incompatible `DataScope`s), and if the scopes\nare incompatible we must align the rows in each column by performing one\nor more joins before calling the function.\n\nNoe: the engine's planned expressions are a tree of \"scoped exprs\" where\neach node in the tree represents a datafusion expression operating on a\nsingle data scope. While evaluating the tree at each node, we take data\nfrom the source and possibly perform \"joins\" on the input (depending on\nthe data source for this node), to create an input record batch for the\ndatafusion expression. A 2-way join was already implemented for binary\nexpressions (using in arithmetic, for example).\n\nThis PR adds a new kind of data source called `MultiJoin` representing\nan arbitrary number of input expressions that must be joined, and having\nthe convention that the resulting record batch columns will be named\nlike \"arg0\", \"arg1\", ... \"argn\". The planner is changed to create this\nkind of join for as the data source to a scoped expr node that evaluates\na function if any of the args have incompatible `DataScope`s. The scoped\nexpr node now has logic to perform this `MultiJoin` when it encounters\ndata source of this type. The mutli join logic is implemented in the\n`pipeline::expr::join` module, using the same join utilities we use for\nbinary expressions.\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Closes #2634\n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nYes, users of transform processor can now pass to functions columns as\narguments that in OTAP would have different row orders.\n\n <!-- If yes, provide further info below -->",
          "timestamp": "2026-04-14T22:10:46Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/706037b6dd9d5707299b6f84ca31c1e6942a2069"
        },
        "date": 1776222678048,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 105.47,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Laurent Quérel",
            "username": "lquerel",
            "email": "l.querel@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "f6dbd384b8013868513f25fd5955a988c17a2eba",
          "message": "Fix topic shutdown and mixed publish semantics (#2631)\n\n## Change Summary\n\nThis PR extracts the topic-runtime fixes and refactorings from the\nlarger live-reconfiguration branch into a standalone change.\n\nThe core design decision in this PR is to move topic-specific waiting\nand delivery semantics back into the topic runtime instead of emulating\nthem in the topic receiver and topic exporter. Before this change, the\nnode layer carried its own retry/wait loops to handle blocked publish,\ndownstream backpressure, and shutdown/drain behavior. **That made\nshutdown under load fragile and allowed topic semantics to diverge\nacross code paths**.\n\nConcretely, this PR does four things:\n\n1. It makes the topic runtime own blocked publish waiting.\n- `publish(...)` / `publish_tracked(...)` now wait inside the topic\nimplementation instead of relying on node-side polling loops.\n- This removes the old timer-driven retry behavior from the topic\nexporter hot path and keeps `queue_on_full: block` wake-driven and\nshutdown-responsive.\n\n2. It introduces delivery leases on the subscribe side and uses them in\nthe topic receiver.\n- The receiver now distinguishes “the topic produced a message” from\n“the receiver successfully handed it downstream”.\n- That allows `DrainIngress` to stop polling the topic immediately while\nstill waiting for already-forwarded tracked messages to reach their\nterminal Ack/Nack outcome.\n- This fixes the previous shutdown bug where the topic receiver could\nreport drained state while continuing to run under load.\n\n3. It tightens mixed-topic semantics so publish behavior is consistent\nacross code paths.\n- Mixed `try_publish` is now all-or-nothing across balanced and\nbroadcast delivery.\n- If balanced delivery cannot admit a message, broadcast subscribers do\nnot receive it either.\n- Mixed async publish was also changed to avoid holding balanced-group\npermits while waiting on another full group, which removes the previous\npartial-permit convoy behavior.\n\n4. It keeps the refactor performance-oriented rather than purely\nstructural.\n- The topic receiver/exporter fast paths still try immediate\nforward/publish first.\n- The in-memory delivery-lease path now uses specialized inline storage\ninstead of per-message boxing on the common path.\n- The opaque fallback is still preserved for future non-in-memory\nbackends, but the in-memory backend no longer pays that allocation cost\nper delivered message.\n\n**This PR does not add new public routes, config knobs, or admin APIs.**\nThe existing topic publish/subscribe APIs remain source-compatible; the\nmain changes are semantic correctness under backpressure/shutdown and\nsimplification of where the waiting logic lives.\n\n## What issue does this PR close?\n\n* Closes\n[#2630](https://github.com/open-telemetry/otel-arrow/issues/2630)\n\n## How are these changes tested?\n\n- `cargo xtask check`\n\nKey topic/runtime guarantees covered by tests:\n- Balanced topics deliver each message exactly once within a subscriber\ngroup.\n- Balanced topics preserve per-subscriber ordering.\n- Broadcast topics deliver all messages to all subscribers in order.\n- Broadcast lag handling is isolated to the lagging subscriber and does\nnot block healthy subscribers.\n- Mixed async publish does not expose a message to broadcast subscribers\nbefore balanced admission succeeds.\n- Mixed `try_publish` is all-or-nothing: if balanced admission fails,\nbroadcast delivery does not happen.\n- Mixed async publish does not reserve permits in unrelated balanced\ngroups while waiting on a full group.\n- Dropping a blocked mixed publish does not leak a partial broadcast\ndelivery.\n- Blocking tracked publish does not leak in-flight capacity when\ncanceled.\n- Delivery leases preserve tracked message outcome semantics, including\nabort-to-nack behavior.\n- The in-memory delivery lease fast path uses specialized inline\nstorage, while the opaque fallback path still works.\n- Topic receiver `DrainIngress` exits promptly when Ack/Nack propagation\nis disabled.\n- Topic receiver `DrainIngress` waits for already-forwarded tracked work\nto complete when Ack/Nack propagation is enabled.\n- Topic receiver drain can interrupt a blocked downstream forward\nwithout hanging shutdown.\n- Topic exporter shutdown interrupts blocked untracked publish under\n`queue_on_full: block`.\n- Topic exporter shutdown interrupts blocked tracked publish under\n`queue_on_full: block`.\n- Topic exporter shutdown force-resolves buffered in-flight pdata it\nstill owns instead of hanging.\n- End-to-end topic exporter -> topic receiver flow still transfers pdata\ncorrectly.\n- End-to-end topic receiver source tagging still works when enabled.\n\nIn addition, this topic work was exercised in the larger\nlive-reconfiguration branch against the scenario that originally exposed\nthe shutdown issue (`topic_multitenant_isolation.yaml`), including\nreplace rollout, per-pipeline shutdown, and group shutdown flows.\n\n## Are there any user-facing changes?\n\nYes.\n\n- Topic-based pipelines shut down more reliably under backpressure.\n- Topic receiver drain behavior is now stricter and better aligned with\nAck/Nack propagation semantics.\n- Mixed-topic publish is now all-or-nothing across balanced and\nbroadcast delivery.\n- There are no new public APIs, config knobs, or route changes in this\nPR.",
          "timestamp": "2026-04-16T00:51:52Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f6dbd384b8013868513f25fd5955a988c17a2eba"
        },
        "date": 1776312215255,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 103.24,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "c1ly",
            "username": "c1ly",
            "email": "129437996+c1ly@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6ef19a99a26f17c89a12f5dad18c737259c1509d",
          "message": "run validation ci manually (#2675)\n\nI am seeing the validation tests acting up again in the validation ci\njob, setting the ci job to be a manual trigger instead of automatically\nrunning on every PR\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-04-16T20:11:07Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/6ef19a99a26f17c89a12f5dad18c737259c1509d"
        },
        "date": 1776395089401,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 102.9,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "c1ly",
            "username": "c1ly",
            "email": "129437996+c1ly@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "495588ee584201ea956c15c6fc102dc3465de675",
          "message": "update container config to allow configurable wait_for setting (#2672)\n\n# Change Summary\n\nUpdate ContainerConfig struct to have a wait_for field of type WaitFor\nfrom the test container crate. Added additional functions to allow a\nuser to configure the WaitFor enum variant to use for a test container\n\n## What issue does this PR close?\n\n* Closes #2668\n\n## How are these changes tested?\n\nunit tests\n\n## Are there any user-facing changes?\n\nno\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-04-17T12:32:22Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/495588ee584201ea956c15c6fc102dc3465de675"
        },
        "date": 1776481139818,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 102.9,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0c13ab96cc8616b62a38d1916eb479b1114c3240",
          "message": "Update dependency pydantic to v2.13.2 (#2684)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n| [pydantic](https://redirect.github.com/pydantic/pydantic)\n([changelog](https://docs.pydantic.dev/latest/changelog/)) | `==2.13.0`\n→ `==2.13.2` |\n![age](https://developer.mend.io/api/mc/badges/age/pypi/pydantic/2.13.2?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/pypi/pydantic/2.13.0/2.13.2?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>pydantic/pydantic (pydantic)</summary>\n\n###\n[`v2.13.2`](https://redirect.github.com/pydantic/pydantic/compare/v2.13.1...v2.13.2)\n\n###\n[`v2.13.1`](https://redirect.github.com/pydantic/pydantic/compare/v2.13.0...v2.13.1)\n\n[Compare\nSource](https://redirect.github.com/pydantic/pydantic/compare/v2.13.0...v2.13.1)\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am every weekday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4xMjAuMiIsInVwZGF0ZWRJblZlciI6IjQzLjEyMy44IiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>",
          "timestamp": "2026-04-18T00:27:24Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/0c13ab96cc8616b62a38d1916eb479b1114c3240"
        },
        "date": 1776568299775,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 102.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "renovate[bot]",
            "username": "renovate[bot]",
            "email": "29139614+renovate[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0c13ab96cc8616b62a38d1916eb479b1114c3240",
          "message": "Update dependency pydantic to v2.13.2 (#2684)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n| [pydantic](https://redirect.github.com/pydantic/pydantic)\n([changelog](https://docs.pydantic.dev/latest/changelog/)) | `==2.13.0`\n→ `==2.13.2` |\n![age](https://developer.mend.io/api/mc/badges/age/pypi/pydantic/2.13.2?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/pypi/pydantic/2.13.0/2.13.2?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>pydantic/pydantic (pydantic)</summary>\n\n###\n[`v2.13.2`](https://redirect.github.com/pydantic/pydantic/compare/v2.13.1...v2.13.2)\n\n###\n[`v2.13.1`](https://redirect.github.com/pydantic/pydantic/compare/v2.13.0...v2.13.1)\n\n[Compare\nSource](https://redirect.github.com/pydantic/pydantic/compare/v2.13.0...v2.13.1)\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am every weekday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4xMjAuMiIsInVwZGF0ZWRJblZlciI6IjQzLjEyMy44IiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>",
          "timestamp": "2026-04-18T00:27:24Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/0c13ab96cc8616b62a38d1916eb479b1114c3240"
        },
        "date": 1776654736446,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 102.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Aaron Marten",
            "username": "AaronRM",
            "email": "AaronRM@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1e251126b4f960b4022cfa65b59ee21a50764def",
          "message": "test: Add JUnit XML upload and flaky test tracking workflow (#2699)\n\n# Change Summary\n\n- Implemented JUnit XML result uploads for both required and\nnon-required tests in the Rust-CI workflow.\n- Created a new workflow for detecting flaky tests from JUnit XML\nartifacts, which runs daily and on-demand.\n- The flaky test tracker parses JUnit results, identifies flaky tests,\nand creates or updates a tracking issue with a summary.\n\n## What issue does this PR close?\n\nn/a\n\n## How are these changes tested?\n\nn/a. This is adding a new flaky test detection workflow. No changes to\nproduct code.\n\n## Are there any user-facing changes?\n\nNo. Test/infra only.\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>",
          "timestamp": "2026-04-20T18:48:02Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1e251126b4f960b4022cfa65b59ee21a50764def"
        },
        "date": 1776740757333,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 102.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Mikel Blanchard",
            "username": "CodeBlanch",
            "email": "mblanchard@macrosssoftware.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "dd344850b0de4426753c6c5ac7ca8786a2545458",
          "message": "[query-engine] Tweak slice validation errors (#2721)\n\nRelates to #2636\n\n# Changes\n\n* Tweak the slice validation error messages\n\n---------\n\nCo-authored-by: Drew Relmas <drewrelmas@gmail.com>",
          "timestamp": "2026-04-21T19:39:49Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/dd344850b0de4426753c6c5ac7ca8786a2545458"
        },
        "date": 1776827025549,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 103,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Pritish Nahar",
            "username": "pritishnahar95",
            "email": "pritishnahar@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "446e2510d411d9944a3a37faba577e2029677e8b",
          "message": "feat(config): add optional field to EngineConfig (#2727)\n\nAllow applications embedding the dataflow engine to carry their own\nengine-level configuration under `engine.custom`. The engine ignores\nthis field entirely; embedding binaries can read namespaced keys for\nconcerns like remote management, auth, or fleet coordination.\n\n# Change Summary\n\nAdd an optional `custom: HashMap<String, serde_json::Value>` field to\n`EngineConfig`. This gives embedding applications an escape hatch for\nengine-level config without forking the config crate or pre-parsing\nYAML. The field defaults to an empty map and is omitted from serialized\noutput when empty.\n\n## What issue does this PR close?\n\n* Closes #2561\n\n## How are these changes tested?\n\nThree new unit tests in `engine.rs`:\n- `from_yaml_accepts_custom_config` — parses a config with multiple\nnamespaced custom keys and verifies values\n- `custom_defaults_to_empty` — confirms the field defaults to an empty\nmap when omitted\n- `custom_roundtrips_through_json` — serializes to JSON and deserializes\nback, verifying data is preserved\n\n## Are there any user-facing changes?\n\nYes. A new optional `custom` key is available under the `engine` section\nof the YAML/JSON config. Example:\n\n```yaml\nengine:\n  custom:\n    remote_management:\n      server_url: \"ws://mgmt.example.com/v1\"\n      heartbeat_interval_secs: 10\n    custom_auth:\n      provider: \"oidc\"\n      token_endpoint: \"https://auth.example.com/token\"\n```\n\nExisting configs are unaffected since the field defaults to empty.\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>",
          "timestamp": "2026-04-22T23:05:42Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/446e2510d411d9944a3a37faba577e2029677e8b"
        },
        "date": 1776913623454,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 103.02,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}