window.BENCHMARK_DATA = {
  "lastUpdate": 1787991638761,
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
      },
      {
        "commit": {
          "author": {
            "name": "sapatrjv",
            "username": "sapatrjv",
            "email": "sapatrjv@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "da7688be82431d7cf4508c7376036fb240785034",
          "message": "Use latest weaver build and simplify crypto selection. (#2740)\n\n# Change Summary\n\n<!--\nUse latest weaver build that has the exclusion of openssl build in case\nof windows platforms. In case of windows platforms it uses SChannel TLS\ninstead of natively building openssl.\n\nSimplification of crypto selection.\n\n-->\n\n## What issue does this PR close?\n\nPart of https://github.com/open-telemetry/otel-arrow/issues/2697\n\n## How are these changes tested?\n\nSearch cargo tree and check on windows platform no openssl dependency.\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>",
          "timestamp": "2026-04-24T00:33:27Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/da7688be82431d7cf4508c7376036fb240785034"
        },
        "date": 1777000144747,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 106.52,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marc Snider",
            "username": "marcsnid",
            "email": "30638925+marcsnid@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "bc4cec8b9dd31adbf3a207903eb652c16499467c",
          "message": "Added debug assertions for negative values in counter and histograms (#2757)\n\n# Change Summary\n\nAdd `debug_assert!` checks to enforce non-negative values in\n`Counter<f64>` and `Mmsc` (Histogram bridge) instruments in\n`otap_df_telemetry`. Counters and Histogram-based instruments must only\nreceive non-negative deltas for correctness. Their sums are exported as\nPrometheus counters, which require monotonicity.\n\nThree guards added:\n- `Counter<f64>::add(v)` — asserts `v >= 0.0`\n- `AddAssign<f64> for Counter<f64>` — asserts `rhs >= 0.0`\n- `Mmsc::record(value)` — asserts `value >= 0.0`\n\nThese use `debug_assert!` (zero cost in release builds) per the issue\ndiscussion.\n\n## What issue does this PR close?\n#2100\n\n## How are these changes tested?\n\n- Replaced the existing `test_mmsc_negative_values` test (which\nvalidated now-invalid behavior) with three `#[cfg(debug_assertions)]\n#[should_panic]` tests that verify the assertions fire on negative\ninput:\n  - `test_mmsc_record_rejects_negative`\n  - `test_counter_f64_add_rejects_negative`\n  - `test_counter_f64_add_assign_rejects_negative`\n- Tests in `otap-df-telemetry` continue to pass.\n\n## Are there any user-facing changes?\n\nNo. `debug_assert!` is stripped in release builds. In debug builds,\npassing a negative value to `Counter<f64>::add()`, `Counter<f64> +=`, or\n`Mmsc::record()` will now panic with a descriptive message, catching\nincorrect usage during development.",
          "timestamp": "2026-04-24T23:10:51Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/bc4cec8b9dd31adbf3a207903eb652c16499467c"
        },
        "date": 1777085773850,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 103.99,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marc Snider",
            "username": "marcsnid",
            "email": "30638925+marcsnid@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "bc4cec8b9dd31adbf3a207903eb652c16499467c",
          "message": "Added debug assertions for negative values in counter and histograms (#2757)\n\n# Change Summary\n\nAdd `debug_assert!` checks to enforce non-negative values in\n`Counter<f64>` and `Mmsc` (Histogram bridge) instruments in\n`otap_df_telemetry`. Counters and Histogram-based instruments must only\nreceive non-negative deltas for correctness. Their sums are exported as\nPrometheus counters, which require monotonicity.\n\nThree guards added:\n- `Counter<f64>::add(v)` — asserts `v >= 0.0`\n- `AddAssign<f64> for Counter<f64>` — asserts `rhs >= 0.0`\n- `Mmsc::record(value)` — asserts `value >= 0.0`\n\nThese use `debug_assert!` (zero cost in release builds) per the issue\ndiscussion.\n\n## What issue does this PR close?\n#2100\n\n## How are these changes tested?\n\n- Replaced the existing `test_mmsc_negative_values` test (which\nvalidated now-invalid behavior) with three `#[cfg(debug_assertions)]\n#[should_panic]` tests that verify the assertions fire on negative\ninput:\n  - `test_mmsc_record_rejects_negative`\n  - `test_counter_f64_add_rejects_negative`\n  - `test_counter_f64_add_assign_rejects_negative`\n- Tests in `otap-df-telemetry` continue to pass.\n\n## Are there any user-facing changes?\n\nNo. `debug_assert!` is stripped in release builds. In debug builds,\npassing a negative value to `Counter<f64>::add()`, `Counter<f64> +=`, or\n`Mmsc::record()` will now panic with a descriptive message, catching\nincorrect usage during development.",
          "timestamp": "2026-04-24T23:10:51Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/bc4cec8b9dd31adbf3a207903eb652c16499467c"
        },
        "date": 1777173214397,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 103.99,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Marc Snider",
            "username": "marcsnid",
            "email": "30638925+marcsnid@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "bc4cec8b9dd31adbf3a207903eb652c16499467c",
          "message": "Added debug assertions for negative values in counter and histograms (#2757)\n\n# Change Summary\n\nAdd `debug_assert!` checks to enforce non-negative values in\n`Counter<f64>` and `Mmsc` (Histogram bridge) instruments in\n`otap_df_telemetry`. Counters and Histogram-based instruments must only\nreceive non-negative deltas for correctness. Their sums are exported as\nPrometheus counters, which require monotonicity.\n\nThree guards added:\n- `Counter<f64>::add(v)` — asserts `v >= 0.0`\n- `AddAssign<f64> for Counter<f64>` — asserts `rhs >= 0.0`\n- `Mmsc::record(value)` — asserts `value >= 0.0`\n\nThese use `debug_assert!` (zero cost in release builds) per the issue\ndiscussion.\n\n## What issue does this PR close?\n#2100\n\n## How are these changes tested?\n\n- Replaced the existing `test_mmsc_negative_values` test (which\nvalidated now-invalid behavior) with three `#[cfg(debug_assertions)]\n#[should_panic]` tests that verify the assertions fire on negative\ninput:\n  - `test_mmsc_record_rejects_negative`\n  - `test_counter_f64_add_rejects_negative`\n  - `test_counter_f64_add_assign_rejects_negative`\n- Tests in `otap-df-telemetry` continue to pass.\n\n## Are there any user-facing changes?\n\nNo. `debug_assert!` is stripped in release builds. In debug builds,\npassing a negative value to `Counter<f64>::add()`, `Counter<f64> +=`, or\n`Mmsc::record()` will now panic with a descriptive message, catching\nincorrect usage during development.",
          "timestamp": "2026-04-24T23:10:51Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/bc4cec8b9dd31adbf3a207903eb652c16499467c"
        },
        "date": 1777261091069,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 103.99,
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
          "id": "d5f4b35d5509da5dbe73e48f914d9220fc2e1a3d",
          "message": "Fix batch byte sizing and wakeup state (#2763)\n\n# Change Summary\n\nFixes batch processor behavior for OTLP byte-sized batches by comparing\n`min_size` against pending bytes instead of item count. Also avoids\nunnecessary wakeup set/cancel work for immediate size flushes, clears\nstale timer state after full drains, and adds wakeup scheduler metrics\nfor attribution.\n\n## What issue does this PR close?\n\n* Closes #NNN\n\n## How are these changes tested?\n\n- cargo xtask check\n- controlled benchmark\n\n## Are there any user-facing changes?",
          "timestamp": "2026-04-27T22:13:59Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d5f4b35d5509da5dbe73e48f914d9220fc2e1a3d"
        },
        "date": 1777347945066,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 103.94,
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
          "id": "0ddaeb436473414388587dd60b58bfddcdf7e226",
          "message": "Add dfctl CLI and TUI for OTAP Dataflow Engine administration (#2756)\n\n# Change Summary\n\nAdds `dfctl`, an admin SDK-based command-line tool for operating local\nand remote OTAP Dataflow Engines.\n\nThe CLI supports engine, group, pipeline, telemetry, rollout, shutdown,\nreconfiguration, diagnosis, bundle, watch, shell completion, automation\nfriendly output, and an interactive TUI for operational workflows.\n\nMore details can be find here ->\nhttps://github.com/lquerel/otel-arrow/blob/962a01e30116433e448ed58b6d8b820e1bcdcd3a/rust/otap-dataflow/crates/enginectl/README.md\n\n## What issue does this PR close?\n\nN/A\n\n## How are these changes tested?\n\n- `cargo xtask check`\n\n## Are there any user-facing changes?\n\nYes, the new CLI by itself.\n\n<img width=\"1888\" height=\"1197\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/6ee53571-b1c5-47b2-bdcd-6d06999ea7d5\"\n/>\n\n<img width=\"1888\" height=\"1197\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/21e6273d-9fc5-423d-b4d8-1472d9f40059\"\n/>",
          "timestamp": "2026-04-29T00:24:10Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/0ddaeb436473414388587dd60b58bfddcdf7e226"
        },
        "date": 1777434243626,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.03,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Max Jacinto",
            "username": "luckymachi",
            "email": "77021922+luckymachi@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "775794600fb4ba7bea406b4e0bb08cd598b10cda",
          "message": "Isolation of setup-protoc jobs from RUST-CI (#2772)\n\n# Change Summary\n\nAs mentioned in issue\n[2768](https://github.com/open-telemetry/otel-arrow/issues/2768),\n`setup-protoc` jobs are dropped in favour of a targeted `compile_proto`\njob.\n\n## What issue does this PR close?\n\nThis issue closes issue\n[2768](https://github.com/open-telemetry/otel-arrow/issues/2768)\n\n---------\n\nCo-authored-by: Drew Relmas <drewrelmas@gmail.com>",
          "timestamp": "2026-04-29T20:45:20Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/775794600fb4ba7bea406b4e0bb08cd598b10cda"
        },
        "date": 1777520682322,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.11,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Henry Stolz",
            "username": "hestolz",
            "email": "43051891+hestolz@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "74bcd9886b81855f59df803b3e8979f35115cceb",
          "message": "fix crate name in otap-dataflow readme (#2790)\n\n# Change Summary\n\nfix crate name in otap-dataflow readme.\n\n## What issue does this PR close?\n\nminor nit.\n\n## How are these changes tested?\n\nN/A\n\n## Are there any user-facing changes?\n\nN/A",
          "timestamp": "2026-04-30T19:39:37Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/74bcd9886b81855f59df803b3e8979f35115cceb"
        },
        "date": 1777608276349,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.11,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Gyan ranjan",
            "username": "gyanranjanpanda",
            "email": "213113461+gyanranjanpanda@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "f018901f3a2ba93bee9d45f6afb1204f90679863",
          "message": "Fix duplicate attribute keys in transform_attributes (#2423)\n\n# Fix Duplicate Attribute Keys in `transform_attributes`\n\n## Changes Made\nThis PR resolves issue #1650 by ensuring that dictionary keys are\ndeduplicated when transformations such as `rename` are applied, as\nrequired by the OpenTelemetry specification (\"Exported maps MUST contain\nonly unique keys by default\").\n\nTo accomplish this while maintaining strict performance requirements, we\nreplaced the previous `RowConverter` deduplication strategy with a new\nhigh-performance, proactive pre-filter:\n- We injected `filter_rename_collisions` into\n`transform_attributes_impl` inside\n`otap-dataflow/crates/pdata/src/otap/transform.rs`.\n- Before a rename is processed, this function reads the `parent_id`s and\ntarget keys. It uses the `IdBitmap` type to find any existing target\nkeys whose `parent_id` maps back to an old key that will be renamed.\n- It proactively strips those collision rows from the batch via\n`arrow::compute::filter_record_batch` *before* the actual transform\nhappens.\n\n## Testing\n- Extended the `AttributesProcessor` unit tests\n(`test_rename_removes_duplicate_keys`) to explicitly verify that\nrenaming an attribute resulting in a collision automatically discards\nduplicate keys.\n- Extended the `AttributesTransformPipelineStage` in `query-engine`\ntests with a parallel case ensuring OPL/KQL query pipelines\n(`project-rename`) properly drop duplicates when resolving duplicates.\n- Refactored `otap_df_pdata` `transform.rs` tests to properly expect\ndeduplicated keys using this plan-based method.\n- Validated logic with `cargo test --workspace --all-features`.\n\n## Validation Results\nAll tests pass. OTel semantic rules surrounding unique mapped keys map\ncleanly through down/upstream processors. The `IdBitmap` intersection\napproach completely resolves the multi-thousand percent `RowConverter`\nperformance regressions, dropping collision resolution overhead to\nessentially zero through efficient bitmap operations.\n\n---------\n\nSigned-off-by: Gyanranjan Panda <gyanranjanpanda438@gmail.com>\nCo-authored-by: Gyan Ranjan Panda <gyanranjanpanda@users.noreply.github.com>\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-05-01T20:08:07Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f018901f3a2ba93bee9d45f6afb1204f90679863"
        },
        "date": 1777691468322,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.19,
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
          "id": "9aa767ee7b26712bbab69e4ecab5db2b22f80f32",
          "message": "Update github workflow dependencies (#2802)\n\n> ℹ️ **Note**\n> \n> This PR body was truncated due to platform limits.\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n|\n[DavidAnson/markdownlint-cli2-action](https://redirect.github.com/DavidAnson/markdownlint-cli2-action)\n| action | minor | `v23.0.0` → `v23.1.0` |\n|\n[EmbarkStudios/cargo-deny-action](https://redirect.github.com/EmbarkStudios/cargo-deny-action)\n| action | patch | `v2.0.15` → `v2.0.17` |\n| [Swatinem/rust-cache](https://redirect.github.com/Swatinem/rust-cache)\n| action | minor | `v2` → `v2.9.1` |\n|\n[actions/create-github-app-token](https://redirect.github.com/actions/create-github-app-token)\n| action | minor | `v3.0.0` → `v3.1.1` |\n| [actions/setup-node](https://redirect.github.com/actions/setup-node) |\naction | minor | `v6.3.0` → `v6.4.0` |\n|\n[actions/upload-artifact](https://redirect.github.com/actions/upload-artifact)\n| action | patch | `v7.0.0` → `v7.0.1` |\n|\n[github/codeql-action](https://redirect.github.com/github/codeql-action)\n| action | patch | `v4.35.1` → `v4.35.3` |\n| [go](https://redirect.github.com/actions/go-versions) | uses-with |\npatch | `1.26.1` → `1.26.2` |\n|\n[step-security/harden-runner](https://redirect.github.com/step-security/harden-runner)\n| action | minor | `v2.16.1` → `v2.19.0` |\n|\n[taiki-e/install-action](https://redirect.github.com/taiki-e/install-action)\n| action | minor | `v2.71.2` → `v2.75.28` |\n\n---\n\n### Release Notes\n\n<details>\n<summary>DavidAnson/markdownlint-cli2-action\n(DavidAnson/markdownlint-cli2-action)</summary>\n\n###\n[`v23.1.0`](https://redirect.github.com/DavidAnson/markdownlint-cli2-action/releases/tag/v23.1.0):\nUpdate markdownlint-cli2 version (markdownlint-cli2 v0.22.1,\nmarkdownlint v0.40.0).\n\n[Compare\nSource](https://redirect.github.com/DavidAnson/markdownlint-cli2-action/compare/v23.0.0...v23.1.0)\n\n</details>\n\n<details>\n<summary>EmbarkStudios/cargo-deny-action\n(EmbarkStudios/cargo-deny-action)</summary>\n\n###\n[`v2.0.17`](https://redirect.github.com/EmbarkStudios/cargo-deny-action/releases/tag/v2.0.17):\nRelease 2.0.17 - cargo-deny 0.19.2\n\n[Compare\nSource](https://redirect.github.com/EmbarkStudios/cargo-deny-action/compare/v2.0.16...v2.0.17)\n\n##### Fixed\n\n-\n[PR#845](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/845)\nfixed structural issues with SARIF output, resolving\n[#&#8203;818](https://redirect.github.com/EmbarkStudios/cargo-deny/issues/818).\nThanks\n[@&#8203;KyleChamberlin](https://redirect.github.com/KyleChamberlin)!\n\n###\n[`v2.0.16`](https://redirect.github.com/EmbarkStudios/cargo-deny-action/releases/tag/v2.0.16):\nRelease 2.0.16 - cargo-deny 0.19.1\n\n[Compare\nSource](https://redirect.github.com/EmbarkStudios/cargo-deny-action/compare/v2.0.15...v2.0.16)\n\n##### Fixed\n\n-\n[PR#833](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/833)\nfixed an issue where the maximum advisory database staleness was over 14\nyears instead of the intended 90 days.\n-\n[PR#839](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/839)\nfixed an issue where unsound advisories would appear for transitive\ndependencies despite requesting them only for workspace dependencies,\nresolving\n[#&#8203;829](https://redirect.github.com/EmbarkStudios/cargo-deny/issues/829).\n-\n[PR#840](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/840)\nresolved\n[#&#8203;797](https://redirect.github.com/EmbarkStudios/cargo-deny/issues/797)\nby passing `--filter-platform` when collecting cargo metadata if only a\nsingle target was requested either in the config or via the command\nline.\n-\n[PR#841](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/841)\nfixed an issue where `--frozen` would not disable fetching of the\nadvisory DB, resolving\n[#&#8203;759](https://redirect.github.com/EmbarkStudios/cargo-deny/issues/759).\n-\n[PR#842](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/842)\nand\n[PR#844](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/844)\nupdated crates. Notably `krates` was updated to resolve two issues with\ncrates being pruned from the graph used when running checks. Resolving\nthese two issues may mean that updating cargo-deny may highlight issues\nthat were previously hidden.\n-\n[EmbarkStudios/krates#106](https://redirect.github.com/EmbarkStudios/krates/issues/106)\nwould fail to pull in crates brought in via a feature if that crate had\nits `lib` target renamed by the package author.\n-\n[EmbarkStudios/krates#109](https://redirect.github.com/EmbarkStudios/krates/issues/109)\nwould fail to bring in optional dependencies if they were brought in by\na weak feature in a crate *also* brought in by a weak feature.\n\n##### Changed\n\n-\n[PR#830](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/830)\nremoved `gix` in favor of shelling out to `git`. This massively improves\nbuild times and eases maintenance as `gix` bumps minor versions quite\nfrequently. If cargo-deny is used in an environment that for some reason\nallows internet access but doesn't have `git` available, the advisory\ndatabase would need to be updated before calling cargo-deny.\n-\n[PR#838](https://redirect.github.com/EmbarkStudios/cargo-deny/pull/838)\nremoved `rustsec` in favor of manually implemented advisory parsing and\nchecking, with a nightly cron job that checks that the implementation\nexactly matches rustsec on the official rustsec advisory db.\n\n</details>\n\n<details>\n<summary>Swatinem/rust-cache (Swatinem/rust-cache)</summary>\n\n###\n[`v2.9.1`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.9.1)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.9.0...v2.9.1)\n\nFix regression in hash calculation\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.9.0...v2.9.1>\n\n###\n[`v2.9.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.9.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.8.2...v2.9.0)\n\n##### What's Changed\n\n- Add support for running rust-cache commands from within a Nix shell by\n[@&#8203;marc0246](https://redirect.github.com/marc0246) in\n[#&#8203;290](https://redirect.github.com/Swatinem/rust-cache/pull/290)\n- Bump taiki-e/install-action from 2.62.57 to 2.62.60 in the actions\ngroup by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;291](https://redirect.github.com/Swatinem/rust-cache/pull/291)\n- Bump the actions group across 1 directory with 5 updates by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;296](https://redirect.github.com/Swatinem/rust-cache/pull/296)\n- Bump the prd-major group with 3 updates by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;294](https://redirect.github.com/Swatinem/rust-cache/pull/294)\n- Bump [@&#8203;types/node](https://redirect.github.com/types/node) from\n24.10.1 to 25.0.2 in the dev-major group by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;295](https://redirect.github.com/Swatinem/rust-cache/pull/295)\n- Consider all installed toolchains in cache key by\n[@&#8203;tamird](https://redirect.github.com/tamird) in\n[#&#8203;293](https://redirect.github.com/Swatinem/rust-cache/pull/293)\n- Compare case-insenitively for full cache key match by\n[@&#8203;kbriggs](https://redirect.github.com/kbriggs) in\n[#&#8203;303](https://redirect.github.com/Swatinem/rust-cache/pull/303)\n- Migrate to `node24` runner by\n[@&#8203;rhysd](https://redirect.github.com/rhysd) in\n[#&#8203;314](https://redirect.github.com/Swatinem/rust-cache/pull/314)\n- Bump the actions group across 1 directory with 7 updates by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;312](https://redirect.github.com/Swatinem/rust-cache/pull/312)\n- Bump the prd-minor group across 1 directory with 2 updates by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;307](https://redirect.github.com/Swatinem/rust-cache/pull/307)\n- Bump [@&#8203;types/node](https://redirect.github.com/types/node) from\n25.0.2 to 25.2.2 in the dev-minor group by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;309](https://redirect.github.com/Swatinem/rust-cache/pull/309)\n\n##### New Contributors\n\n- [@&#8203;marc0246](https://redirect.github.com/marc0246) made their\nfirst contribution in\n[#&#8203;290](https://redirect.github.com/Swatinem/rust-cache/pull/290)\n- [@&#8203;tamird](https://redirect.github.com/tamird) made their first\ncontribution in\n[#&#8203;293](https://redirect.github.com/Swatinem/rust-cache/pull/293)\n- [@&#8203;kbriggs](https://redirect.github.com/kbriggs) made their\nfirst contribution in\n[#&#8203;303](https://redirect.github.com/Swatinem/rust-cache/pull/303)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.8.2...v2.9.0>\n\n###\n[`v2.8.2`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.8.2)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.8.1...v2.8.2)\n\n##### What's Changed\n\n- ci: address lint findings, add zizmor workflow by\n[@&#8203;woodruffw](https://redirect.github.com/woodruffw) in\n[#&#8203;262](https://redirect.github.com/Swatinem/rust-cache/pull/262)\n- feat: Implement ability to disable adding job ID + rust environment\nhashes to cache names by\n[@&#8203;Ryan-Brice](https://redirect.github.com/Ryan-Brice) in\n[#&#8203;279](https://redirect.github.com/Swatinem/rust-cache/pull/279)\n- Don't overwrite env for cargo-metadata call by\n[@&#8203;MaeIsBad](https://redirect.github.com/MaeIsBad) in\n[#&#8203;285](https://redirect.github.com/Swatinem/rust-cache/pull/285)\n\n##### New Contributors\n\n- [@&#8203;woodruffw](https://redirect.github.com/woodruffw) made their\nfirst contribution in\n[#&#8203;262](https://redirect.github.com/Swatinem/rust-cache/pull/262)\n- [@&#8203;Ryan-Brice](https://redirect.github.com/Ryan-Brice) made\ntheir first contribution in\n[#&#8203;279](https://redirect.github.com/Swatinem/rust-cache/pull/279)\n- [@&#8203;MaeIsBad](https://redirect.github.com/MaeIsBad) made their\nfirst contribution in\n[#&#8203;285](https://redirect.github.com/Swatinem/rust-cache/pull/285)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.8.1...v2.8.2>\n\n###\n[`v2.8.1`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.8.1)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.8.0...v2.8.1)\n\n##### What's Changed\n\n- Set empty `CARGO_ENCODED_RUSTFLAGS` in workspace metadata retrieval by\n[@&#8203;ark0f](https://redirect.github.com/ark0f) in\n[#&#8203;249](https://redirect.github.com/Swatinem/rust-cache/pull/249)\n- chore(deps): update dependencies by\n[@&#8203;reneleonhardt](https://redirect.github.com/reneleonhardt) in\n[#&#8203;251](https://redirect.github.com/Swatinem/rust-cache/pull/251)\n- chore: fix dependabot groups by\n[@&#8203;reneleonhardt](https://redirect.github.com/reneleonhardt) in\n[#&#8203;253](https://redirect.github.com/Swatinem/rust-cache/pull/253)\n- Bump the prd-patch group with 2 updates by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;254](https://redirect.github.com/Swatinem/rust-cache/pull/254)\n- chore(dependabot): regenerate and commit dist/ by\n[@&#8203;reneleonhardt](https://redirect.github.com/reneleonhardt) in\n[#&#8203;257](https://redirect.github.com/Swatinem/rust-cache/pull/257)\n- Bump [@&#8203;types/node](https://redirect.github.com/types/node) from\n22.16.3 to 24.2.1 in the dev-major group by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;255](https://redirect.github.com/Swatinem/rust-cache/pull/255)\n- Bump typescript from 5.8.3 to 5.9.2 in the dev-minor group by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;256](https://redirect.github.com/Swatinem/rust-cache/pull/256)\n- Bump actions/setup-node from 4 to 5 in the actions group by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;259](https://redirect.github.com/Swatinem/rust-cache/pull/259)\n- Update README.md by\n[@&#8203;Propfend](https://redirect.github.com/Propfend) in\n[#&#8203;234](https://redirect.github.com/Swatinem/rust-cache/pull/234)\n- Bump [@&#8203;types/node](https://redirect.github.com/types/node) from\n24.2.1 to 24.3.0 in the dev-minor group by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;258](https://redirect.github.com/Swatinem/rust-cache/pull/258)\n\n##### New Contributors\n\n- [@&#8203;ark0f](https://redirect.github.com/ark0f) made their first\ncontribution in\n[#&#8203;249](https://redirect.github.com/Swatinem/rust-cache/pull/249)\n- [@&#8203;reneleonhardt](https://redirect.github.com/reneleonhardt)\nmade their first contribution in\n[#&#8203;251](https://redirect.github.com/Swatinem/rust-cache/pull/251)\n- [@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot]\nmade their first contribution in\n[#&#8203;254](https://redirect.github.com/Swatinem/rust-cache/pull/254)\n- [@&#8203;Propfend](https://redirect.github.com/Propfend) made their\nfirst contribution in\n[#&#8203;234](https://redirect.github.com/Swatinem/rust-cache/pull/234)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2...v2.8.1>\n\n###\n[`v2.8.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.8.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.8...v2.8.0)\n\n##### What's Changed\n\n- Add cache-workspace-crates feature by\n[@&#8203;jbransen](https://redirect.github.com/jbransen) in\n[#&#8203;246](https://redirect.github.com/Swatinem/rust-cache/pull/246)\n- Feat: support warpbuild cache provider by\n[@&#8203;stegaBOB](https://redirect.github.com/stegaBOB) in\n[#&#8203;247](https://redirect.github.com/Swatinem/rust-cache/pull/247)\n\n##### New Contributors\n\n- [@&#8203;jbransen](https://redirect.github.com/jbransen) made their\nfirst contribution in\n[#&#8203;246](https://redirect.github.com/Swatinem/rust-cache/pull/246)\n- [@&#8203;stegaBOB](https://redirect.github.com/stegaBOB) made their\nfirst contribution in\n[#&#8203;247](https://redirect.github.com/Swatinem/rust-cache/pull/247)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.7.8...v2.8.0>\n\n###\n[`v2.7.8`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.7.8)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.7...v2.7.8)\n\n##### What's Changed\n\n- Include CPU arch in the cache key for arm64 Linux runners by\n[@&#8203;rhysd](https://redirect.github.com/rhysd) in\n[#&#8203;228](https://redirect.github.com/Swatinem/rust-cache/pull/228)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.7.7...v2.7.8>\n\n###\n[`v2.7.7`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.7.7)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.6...v2.7.7)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.7.6...v2.7.7>\n\n###\n[`v2.7.6`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.7.6)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.5...v2.7.6)\n\n##### What's Changed\n\n- Updated artifact upload action to v4 by\n[@&#8203;guylamar2006](https://redirect.github.com/guylamar2006) in\n[#&#8203;212](https://redirect.github.com/Swatinem/rust-cache/pull/212)\n- Adds an option to do lookup-only of the cache by\n[@&#8203;danlec](https://redirect.github.com/danlec) in\n[#&#8203;217](https://redirect.github.com/Swatinem/rust-cache/pull/217)\n- add runner OS in cache key by\n[@&#8203;rnbguy](https://redirect.github.com/rnbguy) in\n[#&#8203;220](https://redirect.github.com/Swatinem/rust-cache/pull/220)\n- Allow opting out of caching $CARGO\\_HOME/bin. by\n[@&#8203;benjyw](https://redirect.github.com/benjyw) in\n[#&#8203;216](https://redirect.github.com/Swatinem/rust-cache/pull/216)\n\n##### New Contributors\n\n- [@&#8203;guylamar2006](https://redirect.github.com/guylamar2006) made\ntheir first contribution in\n[#&#8203;212](https://redirect.github.com/Swatinem/rust-cache/pull/212)\n- [@&#8203;danlec](https://redirect.github.com/danlec) made their first\ncontribution in\n[#&#8203;217](https://redirect.github.com/Swatinem/rust-cache/pull/217)\n- [@&#8203;rnbguy](https://redirect.github.com/rnbguy) made their first\ncontribution in\n[#&#8203;220](https://redirect.github.com/Swatinem/rust-cache/pull/220)\n- [@&#8203;benjyw](https://redirect.github.com/benjyw) made their first\ncontribution in\n[#&#8203;216](https://redirect.github.com/Swatinem/rust-cache/pull/216)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.7.5...v2.7.6>\n\n###\n[`v2.7.5`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.7.5)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.3...v2.7.5)\n\n##### What's Changed\n\n- Upgrade checkout action from version 3 to 4 by\n[@&#8203;carsten-wenderdel](https://redirect.github.com/carsten-wenderdel)\nin\n[#&#8203;190](https://redirect.github.com/Swatinem/rust-cache/pull/190)\n- fix: usage of `deprecated` version of `node` by\n[@&#8203;hamirmahal](https://redirect.github.com/hamirmahal) in\n[#&#8203;197](https://redirect.github.com/Swatinem/rust-cache/pull/197)\n- Only run macOsWorkaround() on macOS by\n[@&#8203;heksesang](https://redirect.github.com/heksesang) in\n[#&#8203;206](https://redirect.github.com/Swatinem/rust-cache/pull/206)\n- Support Cargo.lock format cargo-lock v4 by\n[@&#8203;NobodyXu](https://redirect.github.com/NobodyXu) in\n[#&#8203;211](https://redirect.github.com/Swatinem/rust-cache/pull/211)\n\n##### New Contributors\n\n-\n[@&#8203;carsten-wenderdel](https://redirect.github.com/carsten-wenderdel)\nmade their first contribution in\n[#&#8203;190](https://redirect.github.com/Swatinem/rust-cache/pull/190)\n- [@&#8203;hamirmahal](https://redirect.github.com/hamirmahal) made\ntheir first contribution in\n[#&#8203;197](https://redirect.github.com/Swatinem/rust-cache/pull/197)\n- [@&#8203;heksesang](https://redirect.github.com/heksesang) made their\nfirst contribution in\n[#&#8203;206](https://redirect.github.com/Swatinem/rust-cache/pull/206)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.7.3...v2.7.5>\n\n###\n[`v2.7.3`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.7.3)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.2...v2.7.3)\n\n- Work around upstream problem that causes cache saving to hang for\nminutes.\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.7.2...v2.7.3>\n\n###\n[`v2.7.2`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.7.2)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.1...v2.7.2)\n\n##### What's Changed\n\n- Update action runtime to `node20` by\n[@&#8203;rhysd](https://redirect.github.com/rhysd) in\n[#&#8203;175](https://redirect.github.com/Swatinem/rust-cache/pull/175)\n- Only key by `Cargo.toml` and `Cargo.lock` files of workspace members\nby [@&#8203;max-heller](https://redirect.github.com/max-heller) in\n[#&#8203;180](https://redirect.github.com/Swatinem/rust-cache/pull/180)\n\n##### New Contributors\n\n- [@&#8203;rhysd](https://redirect.github.com/rhysd) made their first\ncontribution in\n[#&#8203;175](https://redirect.github.com/Swatinem/rust-cache/pull/175)\n- [@&#8203;max-heller](https://redirect.github.com/max-heller) made\ntheir first contribution in\n[#&#8203;180](https://redirect.github.com/Swatinem/rust-cache/pull/180)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.7.1...v2.7.2>\n\n###\n[`v2.7.1`](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.0...v2.7.1)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.7.0...v2.7.1)\n\n###\n[`v2.7.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.7.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.6.2...v2.7.0)\n\n##### What's Changed\n\n- Fix save-if documentation in readme by\n[@&#8203;rukai](https://redirect.github.com/rukai) in\n[#&#8203;166](https://redirect.github.com/Swatinem/rust-cache/pull/166)\n- Support for `trybuild` and similar macro testing tools by\n[@&#8203;neysofu](https://redirect.github.com/neysofu) in\n[#&#8203;168](https://redirect.github.com/Swatinem/rust-cache/pull/168)\n\n##### New Contributors\n\n- [@&#8203;rukai](https://redirect.github.com/rukai) made their first\ncontribution in\n[#&#8203;166](https://redirect.github.com/Swatinem/rust-cache/pull/166)\n- [@&#8203;neysofu](https://redirect.github.com/neysofu) made their\nfirst contribution in\n[#&#8203;168](https://redirect.github.com/Swatinem/rust-cache/pull/168)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.6.2...v2.7.0>\n\n###\n[`v2.6.2`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.6.2)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.6.1...v2.6.2)\n\n##### What's Changed\n\n- dep: Use `smol-toml` instead of `toml` by\n[@&#8203;NobodyXu](https://redirect.github.com/NobodyXu) in\n[#&#8203;164](https://redirect.github.com/Swatinem/rust-cache/pull/164)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2...v2.6.2>\n\n###\n[`v2.6.1`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.6.1)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.6.0...v2.6.1)\n\n- Fix hash contributions of `Cargo.lock`/`Cargo.toml` files.\n\n###\n[`v2.6.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.6.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.5.1...v2.6.0)\n\n##### What's Changed\n\n- Add \"buildjet\" as a second `cache-provider` backend\n[@&#8203;joroshiba](https://redirect.github.com/joroshiba) in\n[#&#8203;154](https://redirect.github.com/Swatinem/rust-cache/pull/154)\n- Clean up sparse registry index.\n- Do not clean up src of `-sys` crates.\n- Remove `.cargo/credentials.toml` before saving.\n\n##### New Contributors\n\n- [@&#8203;joroshiba](https://redirect.github.com/joroshiba) made their\nfirst contribution in\n[#&#8203;154](https://redirect.github.com/Swatinem/rust-cache/pull/154)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.5.1...v2.6.0>\n\n###\n[`v2.5.1`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.5.1)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.5.0...v2.5.1)\n\n- Fix hash contribution of `Cargo.lock`.\n\n###\n[`v2.5.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.5.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.4.0...v2.5.0)\n\n##### What's Changed\n\n- feat: Rm workspace crates version before caching by\n[@&#8203;NobodyXu](https://redirect.github.com/NobodyXu) in\n[#&#8203;147](https://redirect.github.com/Swatinem/rust-cache/pull/147)\n- feat: Add hash of `.cargo/config.toml` to key by\n[@&#8203;NobodyXu](https://redirect.github.com/NobodyXu) in\n[#&#8203;149](https://redirect.github.com/Swatinem/rust-cache/pull/149)\n\n##### New Contributors\n\n- [@&#8203;NobodyXu](https://redirect.github.com/NobodyXu) made their\nfirst contribution in\n[#&#8203;147](https://redirect.github.com/Swatinem/rust-cache/pull/147)\n\n**Full Changelog**:\n<https://github.com/Swatinem/rust-cache/compare/v2.4.0...v2.5.0>\n\n###\n[`v2.4.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.4.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.3.0...v2.4.0)\n\n- Fix cache key stability.\n- Use 8 character hash components to reduce the key length, making it\nmore readable.\n\n###\n[`v2.3.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.3.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.2.1...v2.3.0)\n\n- Add `cache-all-crates` option, which enables caching of crates\ninstalled by workflows.\n- Add installed packages to cache key, so changes to workflows that\ninstall rust tools are detected and cached properly.\n- Fix cache restore failures due to upstream bug.\n- Fix `EISDIR` error due to globed directories.\n- Update runtime `@actions/cache`, `@actions/io` and dev `typescript`\ndependencies.\n- Update `npm run prepare` so it creates distribution files with the\nright line endings.\n\n###\n[`v2.2.1`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.2.1)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.2.0...v2.2.1)\n\n- Update `@actions/cache` dependency to fix usage of `zstd` compression.\n\n###\n[`v2.2.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.2.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.1.0...v2.2.0)\n\n- Add new `save-if` option to always restore, but only conditionally\nsave the cache.\n\n###\n[`v2.1.0`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.1.0)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.0.2...v2.1.0)\n\n- Only hash `Cargo.{lock,toml}` files in the configured workspace\ndirectories.\n\n###\n[`v2.0.2`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.0.2)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2.0.1...v2.0.2)\n\n- Avoid calling cargo metadata on pre-cleanup.\n- Added `prefix-key`, `cache-directories` and `cache-targets` options.\n\n###\n[`v2.0.1`](https://redirect.github.com/Swatinem/rust-cache/releases/tag/v2.0.1)\n\n[Compare\nSource](https://redirect.github.com/Swatinem/rust-cache/compare/v2...v2.0.1)\n\n- Primarily just updating dependencies to fix GitHub deprecation\nnotices.\n\n</details>\n\n<details>\n<summary>actions/create-github-app-token\n(actions/create-github-app-token)</summary>\n\n###\n[`v3.1.1`](https://redirect.github.com/actions/create-github-app-token/releases/tag/v3.1.1)\n\n[Compare\nSource](https://redirect.github.com/actions/create-github-app-token/compare/v3.1.0...v3.1.1)\n\n##### Bug Fixes\n\n- improve error message when app identifier is empty\n([#&#8203;362](https://redirect.github.com/actions/create-github-app-token/issues/362))\n([07e2b76](https://redirect.github.com/actions/create-github-app-token/commit/07e2b760664f080c40eec4eacf7477256582db36)),\ncloses\n[#&#8203;249](https://redirect.github.com/actions/create-github-app-token/issues/249)\n\n###\n[`v3.1.0`](https://redirect.github.com/actions/create-github-app-token/releases/tag/v3.1.0)\n\n[Compare\nSource](https://redirect.github.com/actions/create-github-app-token/compare/v3...v3.1.0)\n\n##### Bug Fixes\n\n- **deps:** bump p-retry from 7.1.1 to 8.0.0\n([#&#8203;357](https://redirect.github.com/actions/create-github-app-token/issues/357))\n([3bbe07d](https://redirect.github.com/actions/create-github-app-token/commit/3bbe07d928e2d6c30bf3e37c6b89edbc4045facf))\n\n##### Features\n\n- add `client-id` input and deprecate `app-id`\n([#&#8203;353](https://redirect.github.com/actions/create-github-app-token/issues/353))\n([e6bd4e6](https://redirect.github.com/actions/create-github-app-token/commit/e6bd4e6970172bed9fe138b2eaf4cbffa4cca8f9))\n- update permission inputs\n([#&#8203;358](https://redirect.github.com/actions/create-github-app-token/issues/358))\n([076e948](https://redirect.github.com/actions/create-github-app-token/commit/076e9480ca6e9633bff412d05eff0fc2f1e7d2be))\n\n</details>\n\n<details>\n<summary>actions/setup-node (actions/setup-node)</summary>\n\n###\n[`v6.4.0`](https://redirect.github.com/actions/setup-node/compare/v6.3.0...v6.4.0)\n\n[Compare\nSource](https://redirect.github.com/actions/setup-node/compare/v6.3.0...v6.4.0)\n\n</details>\n\n<details>\n<summary>actions/upload-artifact (actions/upload-artifact)</summary>\n\n###\n[`v7.0.1`](https://redirect.github.com/actions/upload-artifact/releases/tag/v7.0.1)\n\n[Compare\nSource](https://redirect.github.com/actions/upload-artifact/compare/v7...v7.0.1)\n\n##### What's Changed\n\n- Update the readme with direct upload details by\n[@&#8203;danwkennedy](https://redirect.github.com/danwkennedy) in\n[#&#8203;795](https://redirect.github.com/actions/upload-artifact/pull/795)\n- Readme: bump all the example versions to v7 by\n[@&#8203;danwkennedy](https://redirect.github.com/danwkennedy) in\n[#&#8203;796](https://redirect.github.com/actions/upload-artifact/pull/796)\n- Include changes in typespec/ts-http-runtime 0.3.5 by\n[@&#8203;yacaovsnc](https://redirect.github.com/yacaovsnc) in\n[#&#8203;797](https://redirect.github.com/actions/upload-artifact/pull/797)\n\n**Full Changelog**:\n<https://github.com/actions/upload-artifact/compare/v7...v7.0.1>\n\n</details>\n\n<details>\n<summary>github/codeql-action (github/codeql-action)</summary>\n\n###\n[`v4.35.3`](https://redirect.github.com/github/codeql-action/releases/tag/v4.35.3)\n\n[Compare\nSource](https://redirect.github.com/github/codeql-action/compare/v4.35.2...v4.35.3)\n\n- *Upcoming breaking change*: Add a deprecation warning for customers\nusing CodeQL version 2.19.3 and earlier. These versions of CodeQL were\ndiscontinued on 9 April 2026 alongside GitHub Enterprise Server 3.15,\nand will be unsupported by the next minor release of the CodeQL Action.\n[#&#8203;3837](https://redirect.github.com/github/codeql-action/pull/3837)\n- Configurations for private registries that use Cloudsmith or GCP OIDC\nare now accepted.\n[#&#8203;3850](https://redirect.github.com/github/codeql-action/pull/3850)\n- Best-effort connection tests for private registries now use `GET`\nrequests instead of `HEAD` for better compatibility with various\nregistry implementations. For NuGet feeds, the test is now always\nperformed against the service index.\n[#&#8203;3853](https://redirect.github.com/github/codeql-action/pull/3853)\n- Fixed a bug where two diagnostics produced within the same millisecond\ncould overwrite each other on disk, causing one of them to be lost.\n[#&#8203;3852](https://redirect.github.com/github/codeql-action/pull/3852)\n- Update default CodeQL bundle version to\n[2.25.3](https://redirect.github.com/github/codeql-action/releases/tag/codeql-bundle-v2.25.3).\n[#&#8203;3865](https://redirect.github.com/github/codeql-action/pull/3865)\n\n###\n[`v4.35.2`](https://redirect.github.com/github/codeql-action/releases/tag/v4.35.2)\n\n[Compare\nSource](https://redirect.github.com/github/codeql-action/compare/v4.35.1...v4.35.2)\n\n- The undocumented TRAP cache cleanup feature that could be enabled\nusing the `CODEQL_ACTION_CLEANUP_TRAP_CACHES` environment variable is\ndeprecated and will be removed in May 2026. If you are affected by this,\nwe recommend disabling TRAP caching by passing the `trap-caching: false`\ninput to the `init` Action.\n[#&#8203;3795](https://redirect.github.com/github/codeql-action/pull/3795)\n- The Git version 2.36.0 requirement for improved incremental analysis\nnow only applies to repositories that contain submodules.\n[#&#8203;3789](https://redirect.github.com/github/codeql-action/pull/3789)\n- Python analysis on GHES no longer extracts the standard library,\nrelying instead on models of the standard library. This should result in\nsignificantly faster extraction and analysis times, while the effect on\nalerts should be minimal.\n[#&#8203;3794](https://redirect.github.com/github/codeql-action/pull/3794)\n- Fixed a bug in the validation of OIDC configurations for private\nregistries that was added in CodeQL Action 4.33.0 / 3.33.0.\n[#&#8203;3807](https://redirect.github.com/github/codeql-action/pull/3807)\n- Update default CodeQL bundle version to\n[2.25.2](https://redirect.github.com/github/codeql-action/releases/tag/codeql-bundle-v2.25.2).\n[#&#8203;3823](https://redirect.github.com/github/codeql-action/pull/3823)\n\n</details>\n\n<details>\n<summary>actions/go-versions (go)</summary>\n\n###\n[`v1.26.2`](https://redirect.github.com/actions/go-versions/releases/tag/1.26.2-24114135105):\n1.26.2\n\n[Compare\nSource](https://redirect.github.com/actions/go-versions/compare/1.26.1-22746851271...1.26.2-24114135105)\n\nGo 1.26.2\n\n</details>\n\n<details>\n<summary>step-security/harden-runner\n(step-security/harden-runner)</summary>\n\n###\n[`v2.19.0`](https://redirect.github.com/step-security/harden-runner/releases/tag/v2.19.0)\n\n[Compare\nSource](https://redirect.github.com/step-security/harden-runner/compare/v2.18.0...v2.19.0)\n\n##### What's Changed\n\n##### New Runner Support\n\nHarden-Runner now supports Depot, Blacksmith, Namespace, and WarpBuild\nrunners with the same egress monitoring, runtime monitoring, and policy\nenforcement available on GitHub-hosted runners.\n\n##### Automated Incident Response for Supply Chain Attacks\n\n- Global block list: Outbound connections to known malicious domains and\nIPs are now blocked even in audit mode.\n- System-defined detection rules: Harden-Runner will trigger lockdown\nmode when a high risk event is detected during an active supply chain\nattack (for example, a process reading the memory of the runner worker\nprocess, a common technique for stealing GitHub Actions secrets).\n\n##### Bug Fixes\n\nWindows and macOS: stability and reliability fixes\n\n**Full Changelog**:\n<https://github.com/step-security/harden-runner/compare/v2.18.0...v2.19.0>\n\n###\n[`v2.18.0`](https://redirect.github.com/step-security/harden-runner/releases/tag/v2.18.0)\n\n[Compare\nSource](https://redirect.github.com/step-security/harden-runner/compare/v2.17.0...v2.18.0)\n\n##### What's Changed\n\nGlobal Block List: During supply chain incidents like the recent axios\nand trivy compromises, StepSecurity will add known malicious domains and\nIP addresses (IOCs) to a global block list. These will be automatically\nblocked, even in audit mode, providing immediate protection without\nrequiring any workflow changes.\n\nDeploy on Self-Hosted VM: Added `deploy-on-self-hosted-vm` input that\nallows the Harden Runner agent to be installed directly on ephemeral\nself-hosted Linux runner VMs at workflow runtime. This is intended as an\nalternative when baking the agent into the VM image is not possible.\n\n**Full Changelog**:\n<https://github.com/step-security/harden-runner/compare/v2.17.0...v2.18.0>\n\n###\n[`v2.17.0`](https://redirect.github.com/step-security/harden-runner/releases/tag/v2.17.0)\n\n[Compare\nSource](https://redirect.github.com/step-security/harden-runner/compare/v2.16.1...v2.17.0)\n\n##### What's Changed\n\n##### Policy Store Support\n\nAdded `use-policy-store` and `api-key` inputs to fetch security policies\ndirectly from the [StepSecurity Policy\nStore](https://docs.stepsecurity.io/harden-runner/policy-store).\nPolicies can be defined and attached at the workflow, repo, org, or\ncluster (ARC) level, with the most granular policy taking precedence.\nThis is the preferred method over the existing `policy` input which\nrequires `id-token: write` permission. If no policy is found in the\nstore, the action defaults to audit mode.\n\n**Full Changelog**:\n<https://github.com/step-security/harden-runner/compare/v2.16.1...v2.17.0>\n\n</details>\n\n<details>\n<summary>taiki-e/install-action (taiki-e/install-action)</summary>\n\n###\n[`v2.75.28`](https://redirect.github.com/taiki-e/install-action/compare/v2.75.27...v2.75.28)\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.27...v2.75.28)\n\n###\n[`v2.75.27`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.27):\n2.75.27\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.26...v2.75.27)\n\n- Update `cargo-udeps@latest` to 0.1.61.\n\n- Update `wasm-tools@latest` to 1.248.0.\n\n- Update `cargo-deb@latest` to 3.6.4.\n\n###\n[`v2.75.26`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.26):\n2.75.26\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.25...v2.75.26)\n\n- Update `wasm-bindgen@latest` to 0.2.120.\n\n- Update `mise@latest` to 2026.4.25.\n\n- Update `martin@latest` to 1.8.0.\n\n- Update `vacuum@latest` to 0.26.4.\n\n###\n[`v2.75.25`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.25):\n2.75.25\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.24...v2.75.25)\n\n- Update `uv@latest` to 0.11.8.\n\n- Update `typos@latest` to 1.45.2.\n\n- Update `tombi@latest` to 0.9.25.\n\n- Update `mise@latest` to 2026.4.24.\n\n###\n[`v2.75.24`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.24):\n2.75.24\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.23...v2.75.24)\n\n- Update `prek@latest` to 0.3.11.\n\n- Update `mise@latest` to 2026.4.23.\n\n- Update `vacuum@latest` to 0.26.3.\n\n###\n[`v2.75.23`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.23):\n2.75.23\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.22...v2.75.23)\n\n- Update `vacuum@latest` to 0.26.2.\n\n- Update `tombi@latest` to 0.9.24.\n\n- Update `mise@latest` to 2026.4.22.\n\n- Update `martin@latest` to 1.7.0.\n\n- Update `git-cliff@latest` to 2.13.1.\n\n- Update `cargo-tarpaulin@latest` to 0.35.4.\n\n- Update `cargo-sort@latest` to 2.1.4.\n\n###\n[`v2.75.22`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.22):\n2.75.22\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.21...v2.75.22)\n\n- Update `tombi@latest` to 0.9.22.\n\n- Update `biome@latest` to 2.4.13.\n\n###\n[`v2.75.21`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.21):\n2.75.21\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.20...v2.75.21)\n\n- Update `mise@latest` to 2026.4.19.\n\n- Update `tombi@latest` to 0.9.21.\n\n- Update `syft@latest` to 1.43.0.\n\n###\n[`v2.75.20`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.20):\n2.75.20\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.19...v2.75.20)\n\n- Update `prek@latest` to 0.3.10.\n\n- Update `cargo-xwin@latest` to 0.22.0.\n\n###\n[`v2.75.19`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.19):\n2.75.19\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.18...v2.75.19)\n\n- Update `wasmtime@latest` to 44.0.0.\n\n- Update `tombi@latest` to 0.9.20.\n\n- Update `martin@latest` to 1.6.0.\n\n- Update `just@latest` to 1.50.0.\n\n- Update `mise@latest` to 2026.4.18.\n\n- Update `rclone@latest` to 1.73.5.\n\n###\n[`v2.75.18`](https://redirect.github.com/taiki-e/install-action/releases/tag/v2.75.18):\n2.75.18\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.17...v2.75.18)\n\n- Update `vacuum@latest` to 0.26.1.\n\n- Update `wasm-tools@latest` to 1.247.0.\n\n- Update `mise@latest` to 2026.4.16.\n\n- Update `espup@latest` to 0.17.1.\n\n- Update `trivy@latest` to 0.70.0.\n\n###\n[`v2.75.17`](https://redirect.github.com/taiki-e/install-action/blob/HEAD/CHANGELOG.md#100---2021-12-30)\n\n[Compare\nSource](https://redirect.github.com/taiki-e/install-action/compare/v2.75.16...v2.75.17)\n\nInitial release\n\n[Unreleased]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.21...HEAD\n\n[2.75.21]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.20...v2.75.21\n\n[2.75.20]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.19...v2.75.20\n\n[2.75.19]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.18...v2.75.19\n\n[2.75.18]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.17...v2.75.18\n\n[2.75.17]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.16...v2.75.17\n\n[2.75.16]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.15...v2.75.16\n\n[2.75.15]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.14...v2.75.15\n\n[2.75.14]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.13...v2.75.14\n\n[2.75.13]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.12...v2.75.13\n\n[2.75.12]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.11...v2.75.12\n\n[2.75.11]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.10...v2.75.11\n\n[2.75.10]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.9...v2.75.10\n\n[2.75.9]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.8...v2.75.9\n\n[2.75.8]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.7...v2.75.8\n\n[2.75.7]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.6...v2.75.7\n\n[2.75.6]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.5...v2.75.6\n\n[2.75.5]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.4...v2.75.5\n\n[2.75.4]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.3...v2.75.4\n\n[2.75.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.2...v2.75.3\n\n[2.75.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.1...v2.75.2\n\n[2.75.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.75.0...v2.75.1\n\n[2.75.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.74.1...v2.75.0\n\n[2.74.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.74.0...v2.74.1\n\n[2.74.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.73.0...v2.74.0\n\n[2.73.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.72.0...v2.73.0\n\n[2.72.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.71.3...v2.72.0\n\n[2.71.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.71.2...v2.71.3\n\n[2.71.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.71.1...v2.71.2\n\n[2.71.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.71.0...v2.71.1\n\n[2.71.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.70.4...v2.71.0\n\n[2.70.4]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.70.3...v2.70.4\n\n[2.70.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.70.2...v2.70.3\n\n[2.70.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.70.1...v2.70.2\n\n[2.70.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.70.0...v2.70.1\n\n[2.70.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.14...v2.70.0\n\n[2.69.14]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.13...v2.69.14\n\n[2.69.13]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.12...v2.69.13\n\n[2.69.12]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.11...v2.69.12\n\n[2.69.11]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.10...v2.69.11\n\n[2.69.10]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.9...v2.69.10\n\n[2.69.9]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.8...v2.69.9\n\n[2.69.8]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.7...v2.69.8\n\n[2.69.7]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.6...v2.69.7\n\n[2.69.6]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.5...v2.69.6\n\n[2.69.5]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.4...v2.69.5\n\n[2.69.4]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.3...v2.69.4\n\n[2.69.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.2...v2.69.3\n\n[2.69.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.1...v2.69.2\n\n[2.69.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.69.0...v2.69.1\n\n[2.69.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.36...v2.69.0\n\n[2.68.36]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.35...v2.68.36\n\n[2.68.35]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.34...v2.68.35\n\n[2.68.34]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.33...v2.68.34\n\n[2.68.33]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.32...v2.68.33\n\n[2.68.32]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.31...v2.68.32\n\n[2.68.31]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.30...v2.68.31\n\n[2.68.30]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.29...v2.68.30\n\n[2.68.29]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.28...v2.68.29\n\n[2.68.28]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.27...v2.68.28\n\n[2.68.27]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.26...v2.68.27\n\n[2.68.26]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.25...v2.68.26\n\n[2.68.25]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.24...v2.68.25\n\n[2.68.24]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.23...v2.68.24\n\n[2.68.23]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.22...v2.68.23\n\n[2.68.22]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.21...v2.68.22\n\n[2.68.21]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.20...v2.68.21\n\n[2.68.20]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.19...v2.68.20\n\n[2.68.19]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.18...v2.68.19\n\n[2.68.18]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.17...v2.68.18\n\n[2.68.17]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.16...v2.68.17\n\n[2.68.16]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.15...v2.68.16\n\n[2.68.15]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.14...v2.68.15\n\n[2.68.14]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.13...v2.68.14\n\n[2.68.13]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.12...v2.68.13\n\n[2.68.12]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.11...v2.68.12\n\n[2.68.11]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.10...v2.68.11\n\n[2.68.10]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.9...v2.68.10\n\n[2.68.9]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.8...v2.68.9\n\n[2.68.8]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.7...v2.68.8\n\n[2.68.7]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.6...v2.68.7\n\n[2.68.6]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.5...v2.68.6\n\n[2.68.5]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.4...v2.68.5\n\n[2.68.4]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.3...v2.68.4\n\n[2.68.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.2...v2.68.3\n\n[2.68.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.1...v2.68.2\n\n[2.68.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.68.0...v2.68.1\n\n[2.68.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.30...v2.68.0\n\n[2.67.30]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.29...v2.67.30\n\n[2.67.29]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.28...v2.67.29\n\n[2.67.28]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.27...v2.67.28\n\n[2.67.27]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.26...v2.67.27\n\n[2.67.26]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.25...v2.67.26\n\n[2.67.25]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.24...v2.67.25\n\n[2.67.24]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.23...v2.67.24\n\n[2.67.23]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.22...v2.67.23\n\n[2.67.22]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.21...v2.67.22\n\n[2.67.21]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.20...v2.67.21\n\n[2.67.20]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.19...v2.67.20\n\n[2.67.19]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.18...v2.67.19\n\n[2.67.18]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.17...v2.67.18\n\n[2.67.17]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.16...v2.67.17\n\n[2.67.16]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.15...v2.67.16\n\n[2.67.15]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.14...v2.67.15\n\n[2.67.14]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.13...v2.67.14\n\n[2.67.13]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.12...v2.67.13\n\n[2.67.12]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.11...v2.67.12\n\n[2.67.11]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.10...v2.67.11\n\n[2.67.10]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.9...v2.67.10\n\n[2.67.9]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.8...v2.67.9\n\n[2.67.8]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.7...v2.67.8\n\n[2.67.7]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.6...v2.67.7\n\n[2.67.6]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.5...v2.67.6\n\n[2.67.5]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.4...v2.67.5\n\n[2.67.4]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.3...v2.67.4\n\n[2.67.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.2...v2.67.3\n\n[2.67.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.1...v2.67.2\n\n[2.67.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.67.0...v2.67.1\n\n[2.67.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.7...v2.67.0\n\n[2.66.7]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.6...v2.66.7\n\n[2.66.6]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.5...v2.66.6\n\n[2.66.5]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.4...v2.66.5\n\n[2.66.4]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.3...v2.66.4\n\n[2.66.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.2...v2.66.3\n\n[2.66.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.1...v2.66.2\n\n[2.66.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.66.0...v2.66.1\n\n[2.66.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.16...v2.66.0\n\n[2.65.16]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.15...v2.65.16\n\n[2.65.15]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.14...v2.65.15\n\n[2.65.14]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.13...v2.65.14\n\n[2.65.13]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.12...v2.65.13\n\n[2.65.12]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.11...v2.65.12\n\n[2.65.11]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.10...v2.65.11\n\n[2.65.10]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.9...v2.65.10\n\n[2.65.9]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.8...v2.65.9\n\n[2.65.8]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.7...v2.65.8\n\n[2.65.7]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.6...v2.65.7\n\n[2.65.6]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.5...v2.65.6\n\n[2.65.5]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.4...v2.65.5\n\n[2.65.4]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.3...v2.65.4\n\n[2.65.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.2...v2.65.3\n\n[2.65.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.1...v2.65.2\n\n[2.65.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.65.0...v2.65.1\n\n[2.65.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.64.2...v2.65.0\n\n[2.64.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.64.1...v2.64.2\n\n[2.64.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.64.0...v2.64.1\n\n[2.64.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.63.3...v2.64.0\n\n[2.63.3]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.63.2...v2.63.3\n\n[2.63.2]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.63.1...v2.63.2\n\n[2.63.1]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.63.0...v2.63.1\n\n[2.63.0]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.67...v2.63.0\n\n[2.62.67]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.66...v2.62.67\n\n[2.62.66]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.65...v2.62.66\n\n[2.62.65]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.64...v2.62.65\n\n[2.62.64]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.63...v2.62.64\n\n[2.62.63]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.62...v2.62.63\n\n[2.62.62]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.61...v2.62.62\n\n[2.62.61]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.60...v2.62.61\n\n[2.62.60]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.59...v2.62.60\n\n[2.62.59]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.58...v2.62.59\n\n[2.62.58]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.57...v2.62.58\n\n[2.62.57]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.56...v2.62.57\n\n[2.62.56]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.55...v2.62.56\n\n[2.62.55]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.54...v2.62.55\n\n[2.62.54]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.53...v2.62.54\n\n[2.62.53]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.52...v2.62.53\n\n[2.62.52]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.51...v2.62.52\n\n[2.62.51]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.50...v2.62.51\n\n[2.62.50]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.49...v2.62.50\n\n[2.62.49]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.48...v2.62.49\n\n[2.62.48]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.47...v2.62.48\n\n[2.62.47]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.46...v2.62.47\n\n[2.62.46]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.45...v2.62.46\n\n[2.62.45]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.44...v2.62.45\n\n[2.62.44]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.43...v2.62.44\n\n[2.62.43]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.42...v2.62.43\n\n[2.62.42]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.41...v2.62.42\n\n[2.62.41]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.40...v2.62.41\n\n[2.62.40]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.39...v2.62.40\n\n[2.62.39]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.38...v2.62.39\n\n[2.62.38]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.37...v2.62.38\n\n[2.62.37]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.36...v2.62.37\n\n[2.62.36]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.35...v2.62.36\n\n[2.62.35]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.34...v2.62.35\n\n[2.62.34]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.33...v2.62.34\n\n[2.62.33]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.32...v2.62.33\n\n[2.62.32]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.31...v2.62.32\n\n[2.62.31]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.30...v2.62.31\n\n[2.62.30]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.29...v2.62.30\n\n[2.62.29]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.28...v2.62.29\n\n[2.62.28]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.27...v2.62.28\n\n[2.62.27]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.26...v2.62.27\n\n[2.62.26]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.25...v2.62.26\n\n[2.62.25]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.24...v2.62.25\n\n[2.62.24]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.23...v2.62.24\n\n[2.62.23]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.22...v2.62.23\n\n[2.62.22]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.21...v2.62.22\n\n[2.62.21]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.20...v2.62.21\n\n[2.62.20]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.19...v2.62.20\n\n[2.62.19]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.18...v2.62.19\n\n[2.62.18]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.17...v2.62.18\n\n[2.62.17]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.16...v2.62.17\n\n[2.62.16]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.15...v2.62.16\n\n[2.62.15]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.14...v2.62.15\n\n[2.62.14]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.13...v2.62.14\n\n[2.62.13]:\nhttps://redirect.github.com/taiki-e/install-action/compare/v2.62.12...v2.62\n\n> ✂ **Note**\n> \n> PR body was truncated to here.\n\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on the first day of the month\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n👻 **Immortal**: This PR will be recreated if closed unmerged. Get\n[config\nhelp](https://redirect.github.com/renovatebot/renovate/discussions) if\nthat's undesired.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4xNTkuMiIsInVwZGF0ZWRJblZlciI6IjQzLjE1OS4yIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-02T11:20:16Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9aa767ee7b26712bbab69e4ecab5db2b22f80f32"
        },
        "date": 1777779987500,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.4,
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
          "id": "fe469abf54b3700a301deeab1cd987722df96382",
          "message": "Update github workflow dependencies (major) (#2803)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n|\n[actions/github-script](https://redirect.github.com/actions/github-script)\n| action | major | `v8.0.0` → `v9.0.0` |\n| [dorny/test-reporter](https://redirect.github.com/dorny/test-reporter)\n| action | major | `v2.7.0` → `v3.0.0` |\n\n---\n\n### Release Notes\n\n<details>\n<summary>actions/github-script (actions/github-script)</summary>\n\n###\n[`v9.0.0`](https://redirect.github.com/actions/github-script/releases/tag/v9.0.0)\n\n[Compare\nSource](https://redirect.github.com/actions/github-script/compare/v8.0.0...v9.0.0)\n\n**New features:**\n\n- **`getOctokit` factory function** — Available directly in the script\ncontext. Create additional authenticated Octokit clients with different\ntokens for multi-token workflows, GitHub App tokens, and cross-org\naccess. See [Creating additional clients with\n`getOctokit`](https://redirect.github.com/actions/github-script#creating-additional-clients-with-getoctokit)\nfor details and examples.\n- **Orchestration ID in user-agent** — The `ACTIONS_ORCHESTRATION_ID`\nenvironment variable is automatically appended to the user-agent string\nfor request tracing.\n\n**Breaking changes:**\n\n- **`require('@&#8203;actions/github')` no longer works in scripts.**\nThe upgrade to `@actions/github` v9 (ESM-only) means\n`require('@&#8203;actions/github')` will fail at runtime. If you\npreviously used patterns like `const { getOctokit } =\nrequire('@&#8203;actions/github')` to create secondary clients, use the\nnew injected `getOctokit` function instead — it's available directly in\nthe script context with no imports needed.\n- `getOctokit` is now an injected function parameter. Scripts that\ndeclare `const getOctokit = ...` or `let getOctokit = ...` will get a\n`SyntaxError` because JavaScript does not allow `const`/`let`\nredeclaration of function parameters. Use the injected `getOctokit`\ndirectly, or use `var getOctokit = ...` if you need to redeclare it.\n- If your script accesses other `@actions/github` internals beyond the\nstandard `github`/`octokit` client, you may need to update those\nreferences for v9 compatibility.\n\n##### What's Changed\n\n- Add ACTIONS\\_ORCHESTRATION\\_ID to user-agent string by\n[@&#8203;Copilot](https://redirect.github.com/Copilot) in\n[#&#8203;695](https://redirect.github.com/actions/github-script/pull/695)\n- ci: use deployment: false for integration test environments by\n[@&#8203;salmanmkc](https://redirect.github.com/salmanmkc) in\n[#&#8203;712](https://redirect.github.com/actions/github-script/pull/712)\n- feat!: add getOctokit to script context, upgrade\n[@&#8203;actions/github](https://redirect.github.com/actions/github) v9,\n[@&#8203;octokit/core](https://redirect.github.com/octokit/core) v7, and\nrelated packages by\n[@&#8203;salmanmkc](https://redirect.github.com/salmanmkc) in\n[#&#8203;700](https://redirect.github.com/actions/github-script/pull/700)\n\n##### New Contributors\n\n- [@&#8203;Copilot](https://redirect.github.com/Copilot) made their\nfirst contribution in\n[#&#8203;695](https://redirect.github.com/actions/github-script/pull/695)\n\n**Full Changelog**:\n<https://github.com/actions/github-script/compare/v8.0.0...v9.0.0>\n\n</details>\n\n<details>\n<summary>dorny/test-reporter (dorny/test-reporter)</summary>\n\n###\n[`v3.0.0`](https://redirect.github.com/dorny/test-reporter/releases/tag/v3.0.0)\n\n[Compare\nSource](https://redirect.github.com/dorny/test-reporter/compare/v2.7.0...v3.0.0)\n\n**Note:** The v3 release requires NodeJS 24 runtime on GitHub Actions\nrunners.\n\n#### What's Changed\n\n- Upgrade action runtime to Node.js 24 by\n[@&#8203;dav-tb](https://redirect.github.com/dav-tb) in\n[#&#8203;738](https://redirect.github.com/dorny/test-reporter/pull/738)\n- Explicitly use lowest permissions required to run workflow by\n[@&#8203;jozefizso](https://redirect.github.com/jozefizso) in\n[#&#8203;745](https://redirect.github.com/dorny/test-reporter/pull/745)\n\n##### Other Changes\n\n- Bump\n[@&#8203;typescript-eslint/parser](https://redirect.github.com/typescript-eslint/parser)\nfrom 8.57.0 to 8.57.1 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;742](https://redirect.github.com/dorny/test-reporter/pull/742)\n- Bump\n[@&#8203;types/adm-zip](https://redirect.github.com/types/adm-zip) from\n0.5.7 to 0.5.8 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;743](https://redirect.github.com/dorny/test-reporter/pull/743)\n- Bump flatted from 3.4.1 to 3.4.2 by\n[@&#8203;dependabot](https://redirect.github.com/dependabot)\\[bot] in\n[#&#8203;744](https://redirect.github.com/dorny/test-reporter/pull/744)\n\n#### New Contributors\n\n- [@&#8203;dav-tb](https://redirect.github.com/dav-tb) made their first\ncontribution in\n[#&#8203;738](https://redirect.github.com/dorny/test-reporter/pull/738)\n\n**Full Changelog**:\n<https://github.com/dorny/test-reporter/compare/v2.7.0...v3.0.0>\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on the first day of the month\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n👻 **Immortal**: This PR will be recreated if closed unmerged. Get\n[config\nhelp](https://redirect.github.com/renovatebot/renovate/discussions) if\nthat's undesired.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4xNTkuMiIsInVwZGF0ZWRJblZlciI6IjQzLjE1OS4yIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-03T03:11:43Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/fe469abf54b3700a301deeab1cd987722df96382"
        },
        "date": 1777866325773,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.4,
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
          "id": "1884c67d731ae445dcab2d6642d7344b37bbad38",
          "message": "fix(otlp_grpc_exporter): Set accept_compressed in addition to send_compressed (#2829)\n\n# Change Summary\n\nSet `accept_compressed` so that we can process responses if the replies\nare compressed with the same codec.\n\n## What issue does this PR close?\n\n\n* Closes #2828\n\n## How are these changes tested?\n\nI tried them locally and verified the warnings in the issue went away.\n\n## Are there any user-facing changes?\n\nNo.",
          "timestamp": "2026-05-04T23:33:25Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1884c67d731ae445dcab2d6642d7344b37bbad38"
        },
        "date": 1777950842820,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.54,
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
          "id": "78856dcb2ecd93270265296c7c279cd9ab877e24",
          "message": "feat(otap-dataflow): Add stopwatch `signals.incoming` and `signals.outgoing` metrics (#2839)\n\n# Change Summary\n\nFollow-up to #2747\n\nAdds two MMSC metrics on the existing `stopwatch` metric set so\noperators can compare signal volume in vs. out across a stopwatch range,\nalongside the existing combined compute duration. \"Signals\" here means\nindividual log records, spans, or metric data points\n(`OtapPdata::num_items()`).\n\n| Metric | Recorded | Why |\n|---|---|---|\n| `stopwatch.signals.incoming` | At the start node, **before**\n`process()` runs | Any filter/drop in the start processor itself does\nnot undercount entry volume. |\n| `stopwatch.signals.outgoing` | At the stop node, **after** `process()`\ncompletes | Reflects what actually leaves the range. |\n\nImplemented as two metric set types (`StopwatchStartMetrics`,\n`StopwatchStopMetrics`) sharing one entity per stopwatch — mirrors the\n`ChannelSenderMetrics` / `ChannelReceiverMetrics` precedent. Each role\nregisters its own `MetricSet` against the same entity and drains its own\naccumulator on the periodic `CollectTelemetry` tick and at shutdown.\n\nTo capture the incoming count pre-process, the existing\n`ProcessorSendHook` trait is renamed to `FlowMeasurementHook` and gains\na default-no-op `after_processor_receive` method. The engine run loops\n(Local + Shared) call it immediately after `inbox.recv_when(...)`\nreturns a `Message::PData`, before `begin_process_timing` and\n`process()`. `OtapPdata` overrides it to drive the start-side counter;\ntest PData stand-ins (`()`, `String`, `TestMsg`) get blanket no-op\nimpls.\n\nThe two hooks fire from different surfaces by design, matching the\nasymmetric flow control of a processor:\n\n| Hook | Fires from | Cardinality per `process()` | Captures |\n|---|---|---|---|\n| `after_processor_receive` | Engine run loop | Exactly 1 (1 dequeue per\niteration) | True pre-process input volume |\n| `before_processor_send` | Effect handler `send_message[_to]` | 0..N\n(drop, pass-through, or fan out) | What actually leaves |\n\n**Behavior change:** removed the `PROCESS_DURATION` gate in\n`build_stopwatch_state`. Stopwatches are already explicit opt-in via the\ntelemetry policy YAML; the gate was redundant and signal counts don't\nneed the timing path. Pipelines with stopwatches under `runtime_metrics:\nbasic`/`none` will now run them instead of silently skipping.\n\n## Demo\n\n`configs/fake-stopwatch-demo.yaml` now includes a 1-in-3\n`processor:log_sampling` node inside the stopwatch range so\n`signals.outgoing` is visibly smaller than `signals.incoming`.\n\n```bash\ncargo run --bin df_engine -- --config configs/fake-stopwatch-demo.yaml\ncurl -s 'http://127.0.0.1:8080/api/v1/telemetry/metrics?format=json' \\\n  | jq '.metric_sets[] | select(.name == \"stopwatch\")'\n```\n\nSample output (truncated, after ~38 collection cycles at 10\nsignals/sec):\n\n```json\n{\n  \"name\": \"stopwatch.signals.incoming\",\n  \"value\": { \"min\": 10.0, \"max\": 10.0, \"sum\": 380.0, \"count\": 38 }\n}\n{\n  \"name\": \"stopwatch.compute.duration\",\n  \"value\": { \"min\": 2859829.0, \"max\": 6619768.0, \"sum\": 170014602.0, \"count\": 38 }\n}\n{\n  \"name\": \"stopwatch.signals.outgoing\",\n  \"value\": { \"min\": 3.0, \"max\": 4.0, \"sum\": 127.0, \"count\": 38 }\n}\n```\n\nReading: 380 signals entered the range (38 batches × 10 signals), 127\nleft it (≈1/3, matching the sampler ratio), and `compute.duration`\naverages ~4.47 ms per batch across the chain (170014602 ns / 38). Both\nsignal-count metrics share the same `stopwatch.name` / `start_node` /\n`stop_node` attributes as the duration metric, so they correlate without\njoins.\n\n## What issue does this PR close?\n\n* Related to #2782 \n* Closes #2837 \n\n## How are these changes tested?\n\nUnit Tests / Local runs\n\n## Are there any user-facing changes?\n\n1. Stopwatch duration metric will now be tracked and emitted even on\n`runtime_metrics: basic/none`.\n2. New Stopwatch metrics for `consumed` and `produced`",
          "timestamp": "2026-05-05T23:00:46Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/78856dcb2ecd93270265296c7c279cd9ab877e24"
        },
        "date": 1778038932604,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.57,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "EMMANUELA OPURUM",
            "username": "Cloud-Architect-Emma",
            "email": "86380966+Cloud-Architect-Emma@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "3ab5efb81801f555a13b4db880b5204aa4b4ec43",
          "message": "feat: support format_datetime scalar function in OPL/OTAP query engine (#2850)\n\nFixes #2835\n\n## Summary\nAdds support for `format_datetime(timestamp, format)` as a scalar\nfunction\nin the OPL/OTAP query engine, allowing queries like:\nlogs | set attributes[\"date\"] = format_datetime(timestamp_unix_nano,\n\"%m/%d/%Y\")\n\n## Implementation\nThis is implemented using DataFusion's built-in `to_char` function,\nwhich\nuses chrono strftime format strings, compatible with OTTL's FormatTime\nformats.\n\n## Changes\n- Added `datetime_expressions` feature to datafusion dependency in\n`Cargo.toml`\n- Added `FORMAT_DATETIME_FUNC_NAME` constant to `consts.rs`\n- Registered `format_datetime` as an external function with 2 parameters\nin `parser.rs`\n- Wired `format_datetime` to DataFusion's `to_char` UDF in `expr.rs\n\n---------\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-05-06T22:30:06Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/3ab5efb81801f555a13b4db880b5204aa4b4ec43"
        },
        "date": 1778125435406,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.61,
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
          "id": "99b02d6115ecd7cea6b1a59fc55711aaf8a0efe0",
          "message": "fix(admin): apply OTel→Prometheus name & unit suffix rules (#2748) (#2900)\n\n# Change Summary\n\nApply the OpenTelemetry → Prometheus name & unit suffix rules in the\nadmin\nHTTP server's Prometheus text exposition.\n\nPer the OpenTelemetry spec for Prometheus exposition:\n\n- Counter metric names must end in `_total`.\n- Unit suffix derived from UCUM (e.g. `By` → `_bytes`, `By/s` →\n  `_bytes_per_second`) is inserted between the base name and `_total`.\n- A base name that already ends in `_total` must not push the unit\nsuffix\n  after it (`errors_total` + `By` → `errors_bytes_total`, not\n  `errors_total_bytes_total`).\n\nThis PR introduces `build_prom_metric_name` and the supporting UCUM\nlookup\ntables/helpers, and routes both `format_prometheus_text` and\n`agg_prometheus_text` through it. `sanitize_prom_metric_name` also now\ncollapses consecutive `_` per spec §Metric Names.\n\nThis PR is the first of two carved out of the original PR #2748. It is\nintentionally focused on metric-name and unit-suffix rules so reviewers\ncan\nevaluate that surface in isolation. Scope-label rename (`set=` →\n`otel_scope_name=`), `target_info` rendering and caching, and label-key\ncollision merging are split into a follow-up PR.\n\n## What issue does this PR close?\n\nPartially addresses #2748. The remainder (scope label, `target_info`,\nlabel\ncollision handling) will be addressed in a follow-up PR.\n\n* Refs #2748\n\n## How are these changes tested?\n\n- 11 new unit tests covering:\n  - `build_prom_metric_name` for counters with units, gauges with units,\n`_total` suffix preservation, the `<base>_<unit>_total` ordering fix,\n    and the `subtotal` (not a real `_total` suffix) edge case.\n  - `has_total_suffix` case-insensitivity.\n  - `ucum_to_prometheus_unit` for simple units, bracketed annotations\n(`{packet}/s`), compound rate units (`By/s`, `KiBy/s`, `m/s`), and the\n    intentional `By/m` rejection (UCUM `m` is meters, not minutes).\n  - `strip_curly_braces` including unbalanced-brace handling.\n  - `sanitize_prom_metric_name` underscore collapsing.\n- The existing `test_agg_prometheus_mmsc_metrics` was updated to assert\nthe\n  new `_milliseconds` unit suffix on its sub-metrics, exercising the new\n  path through `agg_prometheus_text`.\n- Full crate test suite: 33/33 telemetry lib tests pass.\n- `cargo fmt --all -- --check` clean.\n- `cargo clippy -p otap-df-admin --all-targets --all-features` clean.\n\n## Are there any user-facing changes?\n\nYes — the Prometheus text exposed at `/metrics` and\n`/metrics/aggregate` changes shape:\n\n- Counters now end in `_total` (and only one `_total`, even when the\n  source metric name already ended in `_total`).\n- Metric names gain a unit suffix derived from the metric's declared\nUCUM\n  unit, e.g. a metric named `request_duration` with unit `ms` is now\n  exposed as `request_duration_milliseconds` (or\n  `request_duration_milliseconds_total` for counters). MMSC sub-metrics\n  pick up the unit suffix as well, so `request_duration_min` becomes\n  `request_duration_milliseconds_min`.\n- Metrics whose unit is empty or `1` (dimensionless) are unchanged.\n\nDownstream Prometheus scrape consumers that hard-coded the previous\nunit-less metric names will need to update their queries. This is the\nspec-compliant naming and aligns the admin endpoint with what other\nOpenTelemetry → Prometheus exporters produce.",
          "timestamp": "2026-05-08T00:04:08Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/99b02d6115ecd7cea6b1a59fc55711aaf8a0efe0"
        },
        "date": 1778211620379,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 104.93,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Will Butler",
            "username": "wbutler",
            "email": "wbutler@microsoft.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "33569b3c088ffa0fb72f32f5f5007547c00ed460",
          "message": "Fix timeouts in flaky test_no_partitioning UT (#2906)\n\nWorking on a previous merge, I got caught up in a merge validation\nfailure on `test_no_partitioning` in `parquet_exporter`.\n\nLooking at the source, the 1s timeout is defined in `mod.rs` at:\n\n```rust\n        test_runtime\n            .set_exporter(exporter)\n            .run_test(logs_scenario(\n                num_rows,\n                Instant::now().add(Duration::from_secs(1)),\n                Duration::from_secs(1),\n            ))\n```\n\nThe result gets fed into the async block within the `logs_scenario`\nhelper func as `shutdown_timeout`:\n\n```rust\n    fn logs_scenario(\n        num_rows: usize,\n        shutdown_timeout: Instant,\n    ) -> impl FnOnce(TestContext<OtapPdata>) -> Pin<Box<dyn Future<Output = ()>>> {\n        move |ctx| {\n            Box::pin(async move {\n                let mut consumer = Consumer::default();\n                let otap_batch = consumer\n                    .consume_bar(&mut fixtures::create_simple_logs_arrow_record_batches(\n                        SimpleDataGenOptions {\n                            num_rows,\n                            ..Default::default()\n                        },\n                    ))\n                    .unwrap();\n\n                ctx.send_pdata(OtapPdata::new_default(\n                    OtapArrowRecords::Logs(from_record_messages(otap_batch).unwrap()).into(),\n                ))\n                .await\n                .expect(\"Failed to send  logs message\");\n\n                ctx.send_shutdown(shutdown_timeout, \"test completed\")\n                    .await\n                    .unwrap();\n            })\n        }\n    }\n```\n\nBut because the `Instant` is determined at the call time of\n`logs_scenario`, the shutdown timeout clock is already running during\nsetting up the exporter and running the actual test.\n\n## Changes\n\nThe fix is to pass in a `Duration` and then properly start the clock\nwhen we call `send_shutdown` by converting to an `Instant` at that time.\nThis is consistent with the pattern in other UT's, in the module, like\n`test_adaptive_schema_dict_upgrade_write`, `test_metrics`,\n`test_adaptive_schema_optional_columns`, and others, which all compute\nthe `Instant` at the call time of `send_shutdown`.\n\nThis fix also addresses `test_with_partitioning`, which uses the same\nhelper and the same anti-pattern.\n\n## Validation\n\nThe following commands run clean:\n\n`cargo check --workspace`\n`cargo test -p otap-df-core-nodes -- parquet_exporter`\n`cargo clippy -p otap-df-core-nodes --all-targets -- -D warnings`\n`cargo fmt --all -- --check`\n`cargo xtask quick-check`\n\n## Notes\n\n`cargo check -p otap-df-core-nodes` fails on trying to resolve `ring` in\n`crypto`. I'm not going after that as non-germane to this fix.",
          "timestamp": "2026-05-09T00:53:15Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/33569b3c088ffa0fb72f32f5f5007547c00ed460"
        },
        "date": 1778297917389,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 105.07,
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
          "id": "c3cf2beba17f14a89341af778253c77c1a0a4346",
          "message": "Fix validate-configs by renaming Jinja template otap-otap.yaml to .yaml.j2 (#2913)\n\nThe `validate-configs` CI job has been failing on PRs since #2893 landed\n(DFE OTLP baseline templates). The validator script\n(`rust/otap-dataflow/scripts/validate-configs.sh`) discovers configs by\nglobbing every `*.yaml`/`*.yml` containing `version: otel_dataflow/v1`\nand runs `df_engine --validate-and-exit` on each. The new template\n`tools/pipeline_perf_test/test_suites/comparison_dashboard/templates/engine/otap-otap.yaml`\ncontains unrendered Jinja2 placeholders (`{{core_start}}`,\n`{{core_end}}`, …), so the YAML deserializer fails:\n\n```\nError: DeserializationError {\n  format: \"YAML\",\n  details: \"policies.resources.core_allocation.set[0].start: invalid type: map, expected usize at line 12 column 18\"\n}\n```\n\nThe other Jinja templates in the same directory (`otlp-otlp.yaml.j2`,\n`otlphttp-otlphttp.yaml.j2`, `loadgen/*.yaml.j2`, `backend/*.yaml.j2`)\nalready use the `.yaml.j2` extension, which the validator skips. This PR\nbrings `otap-otap.yaml` in line with that convention by renaming it to\n`otap-otap.yaml.j2` and updating the 3 sibling suite files in\n`tools/comparison_dashboard/suites/dfe/` that reference it (the matching\n`dfe-logs-otlp-*-baseline.yaml` files already reference their template\nwith the `.j2` suffix).\n\n## Validation\n\nLocally re-running `./scripts/validate-configs.sh` after the fix:\n\n```\nConfig validation: 73/73 passed, 0 failed.\n```\n\n(Previously 1 failed, 72 passed.)",
          "timestamp": "2026-05-09T23:18:23Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c3cf2beba17f14a89341af778253c77c1a0a4346"
        },
        "date": 1778385014892,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 105.08,
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
          "id": "c3cf2beba17f14a89341af778253c77c1a0a4346",
          "message": "Fix validate-configs by renaming Jinja template otap-otap.yaml to .yaml.j2 (#2913)\n\nThe `validate-configs` CI job has been failing on PRs since #2893 landed\n(DFE OTLP baseline templates). The validator script\n(`rust/otap-dataflow/scripts/validate-configs.sh`) discovers configs by\nglobbing every `*.yaml`/`*.yml` containing `version: otel_dataflow/v1`\nand runs `df_engine --validate-and-exit` on each. The new template\n`tools/pipeline_perf_test/test_suites/comparison_dashboard/templates/engine/otap-otap.yaml`\ncontains unrendered Jinja2 placeholders (`{{core_start}}`,\n`{{core_end}}`, …), so the YAML deserializer fails:\n\n```\nError: DeserializationError {\n  format: \"YAML\",\n  details: \"policies.resources.core_allocation.set[0].start: invalid type: map, expected usize at line 12 column 18\"\n}\n```\n\nThe other Jinja templates in the same directory (`otlp-otlp.yaml.j2`,\n`otlphttp-otlphttp.yaml.j2`, `loadgen/*.yaml.j2`, `backend/*.yaml.j2`)\nalready use the `.yaml.j2` extension, which the validator skips. This PR\nbrings `otap-otap.yaml` in line with that convention by renaming it to\n`otap-otap.yaml.j2` and updating the 3 sibling suite files in\n`tools/comparison_dashboard/suites/dfe/` that reference it (the matching\n`dfe-logs-otlp-*-baseline.yaml` files already reference their template\nwith the `.j2` suffix).\n\n## Validation\n\nLocally re-running `./scripts/validate-configs.sh` after the fix:\n\n```\nConfig validation: 73/73 passed, 0 failed.\n```\n\n(Previously 1 failed, 72 passed.)",
          "timestamp": "2026-05-09T23:18:23Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c3cf2beba17f14a89341af778253c77c1a0a4346"
        },
        "date": 1778472141198,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 105.08,
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
          "id": "d83298856be7ea8aca91e00a095a5861d6629389",
          "message": "Rename remaining meters to drop redundant .metrics suffix (#2917)\n\nFinal cleanup PR for the meter-rename series. Drops the redundant\ntrailing `.metrics` from every remaining meter/scope name across the\nrepo.\n\nCloses #2531. Follow-up to #2879 (`otlp.receiver`), #2888 (`engine`,\n`pipeline`), and #2912 (core-nodes receivers).\n\nA meter name already names a set of metrics, so the trailing `.metrics`\nwas tautological in scrape output and view selectors. Instrument names\nare unchanged — only the scope/meter names.\n\n## Renames in this PR\n\nCore-nodes processors:\n\n- `attributes.processor.metrics` → `attributes.processor`\n- `debug.processor.pdata.metrics` → `debug.processor.pdata`\n- `temporal_reaggregation.processor.pdata.metrics` →\n`temporal_reaggregation.processor.pdata`\n- `content_router.processor.metrics` → `content_router.processor`\n- `signal_type_router.processor.metrics` →\n`signal_type_router.processor`\n- `log_sampling.processor.pdata.metrics` →\n`log_sampling.processor.pdata`\n- `filter.processor.pdata.metrics` → `filter.processor.pdata`\n- `retry.processor.metrics` → `retry.processor`\n- `fanout.processor.metrics` → `fanout.processor`\n\nCore-nodes exporters:\n\n- `perf.exporter.pdata.metrics` → `perf.exporter.pdata`\n- `topic.exporter.metrics` → `topic.exporter`\n\nCore-nodes receivers (added after the original plan):\n\n- `host_metrics.receiver.metrics` → `host_metrics.receiver`\n\nContrib-nodes:\n\n- `azure_monitor_exporter.metrics` → `azure_monitor_exporter`\n- `resource_validator.processor.metrics` →\n`resource_validator.processor`\n\nValidation crate:\n\n- `validation.exporter.metrics` → `validation.exporter`\n- `fanout.processor.metrics` → `fanout.processor`\n\nDoc-only example tweaks (telemetry-macros):\n\n- `my.metrics` → `my` (rustdoc comment in `metric_set` proc-macro)\n- `perf.exporter.pdata.metrics` → `perf.exporter.pdata` (3 places in\n`crates/telemetry-macros/README.md`)\n\n## Intentionally not renamed\n\nLog event names that share the `*.metrics.*` shape (e.g.\n`azure_monitor_exporter.metrics.collect`,\n`pipeline.metrics.reporting.fail`, `tokio.metrics.reporting.fail`,\n`channel.metrics.reporting.fail`, `node.metrics.reporting.fail`). These\nfollow the existing log-event naming convention preserved by PRs #2888 /\n#2912 and are not metric-set names.\n\n## Breaking change for downstream consumers\n\nAnything that selects metrics by **scope/meter name** must be updated.\nInstrument names are unchanged.\n\nExamples:\n\n- View configurations that filter by `ScopeName` (e.g. `ScopeName:\n\"topic.exporter.metrics\"` → `ScopeName: \"topic.exporter\"`).\n- Prometheus relabeling/alerting that keys off the `set=\"…\"` label.\n- Dashboards or queries that group by OTLP scope name.\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-05-11T17:22:03Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d83298856be7ea8aca91e00a095a5861d6629389"
        },
        "date": 1778558030488,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 105.15,
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
          "id": "b645a269cc18cba6310ca15648a990f4bdc94e68",
          "message": "fix: restore uncapped throughput in traffic generator (#2946)\n\nPR #2723 broke uncapped mode — saturation tests dropped from ~290K to\n~1.5K logs/sec. This restores the original behavior.",
          "timestamp": "2026-05-13T00:41:34Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b645a269cc18cba6310ca15648a990f4bdc94e68"
        },
        "date": 1778644600725,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 108.93,
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
          "id": "dbd487cad167f94df0ea1f00758212aeb9293163",
          "message": "feat: add num_connections config to OTLP gRPC exporter (#2967)\n\n## Summary\n\nAdd a `num_connections` configuration option to the OTLP gRPC exporter\nthat controls how many independent TCP connections (tonic Channels) are\ncreated per pipeline.\n\nFixes https://github.com/open-telemetry/otel-arrow/issues/1323\n\n## Problem\n\nWhen the receiver uses `SO_REUSEPORT` across multiple cores, the kernel\ndistributes **new TCP connections** (not individual RPCs) across\nlistener sockets. With the previous behavior of 1 gRPC channel per\npipeline, this caused severe core imbalance — e.g., with 2 engine cores:\none core at 60% and another at 94%.\n\n## Solution\n\n- Added `num_connections` config field (default: 1) to the OTLP gRPC\nexporter\n- When `num_connections > 1`, creates N independent tonic Channels, each\nestablishing its own TCP connection\n- Rewrote `GrpcClientPool` to use a FIFO `VecDeque` for round-robin\ndistribution of gRPC clients across channels\n- Pool is sized to `max(max_in_flight, num_connections)` ensuring every\nchannel gets at least one client\n- Updated saturation test templates to set `num_connections = num_cores\n* 4`\n\n## Results\n\nWith `num_connections` set appropriately:\n- Core imbalance fixed: 60%/94% → 99%/99%\n- 2-core throughput improved from 0.90× to 1.36× of 1-core baseline\n\n| Config | logs/sec | Scaling | Core balance |\n|--------|----------|---------|--------------|\n| 1-core, 1 conn (old) | 164,727 | baseline | N/A |\n| 2-core, 1 conn (old) | 148,685 | 0.90× | 60%/94% |\n| 1-core, 4 conns (new) | 177,461 | baseline | 99.6% |\n| 2-core, 8 conns (new) | 241,964 | 1.36× | 95.4% avg, balanced |\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-05-14T00:38:08Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/dbd487cad167f94df0ea1f00758212aeb9293163"
        },
        "date": 1778730994576,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 108.84,
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
          "id": "40b4f4d1112b1bc55f08185aa778865b4a43bd66",
          "message": "Set cache-bin: false on Swatinem/rust-cache to fix broken cargo on macos-latest (#2978)\n\n## Problem\n\nCI `clippy (*, macos-latest)` (and other macOS rust steps) started\nfailing today across many PRs with:\n\n```\nerror: error: unexpected argument 'clippy' found\nUsage: rustup-init[EXE] [OPTIONS]\n```\n\n## Root cause\n\nGitHub rolled out a new macos-latest runner image today\n([actions/runner-images#14037](https://github.com/actions/runner-images/pull/14037))\nthat changed how the `rustc`/`cargo` rustup proxy binaries are set up.\nCombined with\n[Swatinem/rust-cache#325](https://github.com/Swatinem/rust-cache/pull/325)\n(which made `cache-bin: true` the default in v2.8+), the cached\n`$CARGO_HOME/bin/` from previous runs gets restored over the\nfreshly-installed proxies, leaving `cargo` dispatching to `rustup-init`\nbehavior instead of the real cargo.\n\nTracked upstream:\n[Swatinem/rust-cache#341](https://github.com/Swatinem/rust-cache/issues/341).\n\n## Fix\n\nSet `cache-bin: false` on all 7 `Swatinem/rust-cache` invocations in\n`.github/workflows/rust-ci.yml`. This is the workaround confirmed by the\nupstream issue reporter. We don't `cargo install` any binaries that need\ncaching, so this loses no useful caching.",
          "timestamp": "2026-05-14T22:42:42Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/40b4f4d1112b1bc55f08185aa778865b4a43bd66"
        },
        "date": 1778817557897,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 108.85,
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
          "id": "672d665198917e2ede1ed856a9f88925ef2b151f",
          "message": "perf: add egress_bytes_per_log metric to benchmark reports (#2982)\n\n## Summary\n\nTwo changes to benchmark report metrics:\n\n### 1. Add `egress_bytes_per_log` metric\nAdds a derived metric (bytes/log) computed as `network_tx_bytes_rate /\nlogs_received_rate`. This makes it easy to assess whether compression\nratios in tests are realistic.\n\nFor a ~150 byte log record, realistic values should be ~35-50 bytes/log.\nValues like ~27 bytes/log indicate unrealistic compression from\nlow-entropy test data (e.g., replayed identical payloads).\n\n### 2. Switch network metrics from bytes/sec to MB/s\nReplaces `network_tx_bytes_rate_avg` and `network_rx_bytes_rate_avg`\n(bytes/sec) with `network_tx_mb_per_sec` and `network_rx_mb_per_sec`\n(MB/s) for readability. Raw bytes/sec values like 2,689,390 are hard to\ninterpret at a glance; 2.56 MB/s is immediately meaningful.\n\n### Files changed\n- `integration/configs/integration_report_logs.yaml`\n- `integration/configs/integration_report_metrics.yaml`\n- `integration/configs/integration_report_traces.yaml`\n- `comparison_dashboard/reports/report_logs.yaml`\n- `comparison_dashboard/reports/report_metrics.yaml`\n- `comparison_dashboard/reports/report_traces.yaml`\n\nRelates to #2540",
          "timestamp": "2026-05-15T22:28:24Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/672d665198917e2ede1ed856a9f88925ef2b151f"
        },
        "date": 1778903021710,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 108.85,
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
          "id": "672d665198917e2ede1ed856a9f88925ef2b151f",
          "message": "perf: add egress_bytes_per_log metric to benchmark reports (#2982)\n\n## Summary\n\nTwo changes to benchmark report metrics:\n\n### 1. Add `egress_bytes_per_log` metric\nAdds a derived metric (bytes/log) computed as `network_tx_bytes_rate /\nlogs_received_rate`. This makes it easy to assess whether compression\nratios in tests are realistic.\n\nFor a ~150 byte log record, realistic values should be ~35-50 bytes/log.\nValues like ~27 bytes/log indicate unrealistic compression from\nlow-entropy test data (e.g., replayed identical payloads).\n\n### 2. Switch network metrics from bytes/sec to MB/s\nReplaces `network_tx_bytes_rate_avg` and `network_rx_bytes_rate_avg`\n(bytes/sec) with `network_tx_mb_per_sec` and `network_rx_mb_per_sec`\n(MB/s) for readability. Raw bytes/sec values like 2,689,390 are hard to\ninterpret at a glance; 2.56 MB/s is immediately meaningful.\n\n### Files changed\n- `integration/configs/integration_report_logs.yaml`\n- `integration/configs/integration_report_metrics.yaml`\n- `integration/configs/integration_report_traces.yaml`\n- `comparison_dashboard/reports/report_logs.yaml`\n- `comparison_dashboard/reports/report_metrics.yaml`\n- `comparison_dashboard/reports/report_traces.yaml`\n\nRelates to #2540",
          "timestamp": "2026-05-15T22:28:24Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/672d665198917e2ede1ed856a9f88925ef2b151f"
        },
        "date": 1778990322124,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 108.85,
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
          "id": "672d665198917e2ede1ed856a9f88925ef2b151f",
          "message": "perf: add egress_bytes_per_log metric to benchmark reports (#2982)\n\n## Summary\n\nTwo changes to benchmark report metrics:\n\n### 1. Add `egress_bytes_per_log` metric\nAdds a derived metric (bytes/log) computed as `network_tx_bytes_rate /\nlogs_received_rate`. This makes it easy to assess whether compression\nratios in tests are realistic.\n\nFor a ~150 byte log record, realistic values should be ~35-50 bytes/log.\nValues like ~27 bytes/log indicate unrealistic compression from\nlow-entropy test data (e.g., replayed identical payloads).\n\n### 2. Switch network metrics from bytes/sec to MB/s\nReplaces `network_tx_bytes_rate_avg` and `network_rx_bytes_rate_avg`\n(bytes/sec) with `network_tx_mb_per_sec` and `network_rx_mb_per_sec`\n(MB/s) for readability. Raw bytes/sec values like 2,689,390 are hard to\ninterpret at a glance; 2.56 MB/s is immediately meaningful.\n\n### Files changed\n- `integration/configs/integration_report_logs.yaml`\n- `integration/configs/integration_report_metrics.yaml`\n- `integration/configs/integration_report_traces.yaml`\n- `comparison_dashboard/reports/report_logs.yaml`\n- `comparison_dashboard/reports/report_metrics.yaml`\n- `comparison_dashboard/reports/report_traces.yaml`\n\nRelates to #2540",
          "timestamp": "2026-05-15T22:28:24Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/672d665198917e2ede1ed856a9f88925ef2b151f"
        },
        "date": 1779077559468,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 108.86,
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
          "id": "2d53d89e89d477a39c38d39d863e942167f122ea",
          "message": "docs: document saturation test workload characteristics (#3021)\n\nDocument that saturation tests use static 1KB log bodies with realistic\nentropy (512 unique bodies), distinguishing them from other tests that\nuse semantic_conventions (~300 byte logs). Also removes the stale TODO\nand adds scaling efficiency formula explanation with link to the\nscaling-efficiency benchmark page.",
          "timestamp": "2026-05-18T23:28:48Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2d53d89e89d477a39c38d39d863e942167f122ea"
        },
        "date": 1779163552769,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 109.96,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.53,
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
          "id": "60f251825b8a2022b3c3373761eb6b55f9a30da0",
          "message": "feat(engine): wire extensions and capabilities into runtime pipeline (#2860)\n\n# Change Summary\n \n Part 4 of the Extension System (P1) series. Wires the previously\n landed Capability Registry & Resolver (#2732) into the runtime\n pipeline so extensions are actually instantiated, started, and\n shut down by the engine, and so consumer nodes can resolve their\n capability bindings at build time.\n \n Highlights:\n \n - **Runtime wiring** in `runtime_pipeline.rs`: extension lifecycle\n   is invoked before any data-path node is spawned, and `Shutdown`\n   is delivered to extensions only after the data path drains\n   (\"started first, shut down last\"). Active and passive extensions\n   are handled separately; failures abort startup cleanly.\n - **Local capability ownership aligned with shared** via a\n   Box-clone factory pattern, removing the prior asymmetry between\n   the two trait variants.\n - **Two reference test capabilities** under\n   `crates/engine/src/testing/capability/`: `NoOpStateless` and\n   `NoOpStateful`. They exercise every codegen path of the\n   `#[capability]` proc macro (`&self` × {sync, async}, `&mut self`\n   × {sync, async}, borrowed/owned returns, etc.). Test-only; they\n   intentionally live under `testing/` rather than the public\n   `capability/` surface.\n - **Comprehensive end-to-end test suite** at\n   `crates/engine/tests/extension_e2e.rs` (26 tests) covering:\n   passive/active/background extensions, lifecycle ordering and\n   shutdown ordering, fail-fast on extension errors, dual-variant\n   pruning, one-shot capability enforcement (all accessor\n   combinations), shared mutable state across consumers via\n   `Arc`/`Rc` for both local and shared trait variants, async\n   `&mut self` invocation through boxed handles, and active\n   extensions mutating shared state observed by capability\n   consumers.\n - **Architecture doc** updated with a precise statement of the\n   start-first/shut-down-last invariant (it orders lifecycle\n   *calls*, not init completion) and a noted future consideration\n   to add an opt-in readiness probe if/when an extension needs an\n   init-complete guarantee.\n - **URN unification**: extension URNs now use the canonical\n   4-segment form `urn:<namespace>:extension:<id>` (mirroring the\n   receiver/processor/exporter convention), with a short form\n   `extension:<id>`. The shared parser core lives in a new\n   private `crates/config/src/urn.rs`; `node_urn.rs` and\n   `extension_urn.rs` delegate to it with disjoint accepted-kind\n   sets so the two URN types cannot be confused. As a consequence,\n   `NodeKind::Extension` and the now-unreachable\n   `Error::ExtensionInNodesSection` are removed. Misplacement\n   errors include actionable hints (e.g. *\"declare under\n   `extensions:` instead of `nodes:`\"*).\n - All in-tree node factories (receivers, processors, exporters\n   in `core-nodes` and `contrib-nodes`) updated to accept the new\n   `&Capabilities` parameter; existing factories that don't depend\n   on any capability simply ignore it.\n \n ## What issue does this PR close?\n \n ## How are these changes tested?\n \n - New `extension_e2e.rs` integration test (26 tests) exercises the\n   wiring end-to-end against synthetic receivers/processors/\n   exporters/extensions.\n - New unit tests in `urn.rs` cover the shared parser core and the\n   misplacement-error hints; existing `extension_urn` and\n   `node_urn` tests updated to assert the canonical 4-segment form.\n - Pipeline-level regression tests cover rejecting extension URNs\n   in the `nodes:` section and node URNs in the `extensions:`\n   section.\n - `cargo xtask check` (structure check + `fmt` + `clippy --workspace\n   --all-targets -- -D warnings` + `cargo test --workspace`) passes\n   cleanly. No new clippy warnings.\n \n ## Are there any user-facing changes?\n \n Yes:\n \n - **Extension URN format**: extension URNs now use\n   `urn:<namespace>:extension:<id>` (4-segment) instead of the\n   pre-existing 3-segment `urn:<namespace>:<id>`. Short form\n   `extension:<id>` (expands to `urn:otel:extension:<id>`) is\n   available as a developer convenience. Existing 3-segment\n   extension URNs in pipeline configs must be updated. The\n   previously-bundled `configs/fake-with-extension.yaml` was an\n   orphan (its URN had no registered `ExtensionFactory` anywhere\n   in the binary, and it had no test/script/doc consumers) and\n   was removed in `482feb22c`; the canonical 4-segment shape is\n   covered by the `test_extension_with_config_and_capabilities`\n   unit test in `crates/config/src/pipeline.rs`. A runnable demo\n   config can land in a follow-up alongside a real factory.\n - **New extension authoring surface**: `Extension` trait,\n   `ExtensionWrapper::builder` typestate, the\n   `extension_capabilities!` macro, and the test capabilities\n   `NoOpStateless` / `NoOpStateful` (under `testing/capability/`)\n   are now reachable for external extension authors. The\n   architecture doc captures the lifecycle contract.\n - **Node factory signature** now includes `&Capabilities` as a\n   parameter; existing custom factories will need to accept (and\n   may ignore) this new argument\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-05-19T23:25:05Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/60f251825b8a2022b3c3373761eb6b55f9a30da0"
        },
        "date": 1779249848462,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.84,
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
          "id": "84dfec92b99db44ed2e74a980b1f40df5f4b3ee9",
          "message": "Update one_collect digest to 6ccba44 (#2979)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| one_collect | workspace.dependencies | digest | `cfe3f78` → `6ccba44`\n|\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - At any time (no schedule defined)\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4xNzkuMyIsInVwZGF0ZWRJblZlciI6IjQzLjE4NS4xIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-20T22:51:33Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/84dfec92b99db44ed2e74a980b1f40df5f4b3ee9"
        },
        "date": 1779337622068,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.97,
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
          "id": "877834fbfb606a2f477e58b81b8f7b2653539df5",
          "message": "fix(security): Refactor Go tidy workflows to avoid usage of pull_request_target (#3056)\n\n# Change Summary\n\n### Problem\n\nThe `tidy.yml` workflow uses `pull_request_target` to run `make\ngenotelarrowcol` and push the result back to Renovate/Dependabot PR\nbranches.\n\n`pull_request_target` runs in the context of the base branch, which\nmeans it has access to secrets and write permissions. The current\nworkflow checks out the PR's head ref (`ref: ${{ github.head_ref }}`)\nand then executes code from it (`make genotelarrowcol`). If the\nactor/fork guards were ever bypassed — via a compromised bot account,\nmisconfigured Renovate, or a GitHub bug — an attacker's code would run\nwith full write access and secrets.\n\n### Solution\n\nSplit the workflow into two:\n\n1. **`tidy.yml`** — Triggered by `pull_request` (not\n`pull_request_target`). Runs `make genotelarrowcol` with **no write\npermissions and no secrets**. If changes are detected, uploads a `git\ndiff` patch as an artifact.\n\n2. **`tidy-commit.yml`** — Triggered by `workflow_run` on completion of\nthe first workflow. Downloads the patch, applies it, and pushes. This\nworkflow has write access but **never executes any code from the PR** —\nit only applies a static diff.\n\nThis ensures that code execution and write access never coexist in the\nsame workflow run.\n\n### Before\n\n```\npull_request_target\n  ✓ contents: write, secrets available\n  ✓ checks out + executes PR code\n  ✓ pushes to branch\n```\n\n### After\n\n```\npull_request (tidy.yml)          →  workflow_run (tidy-commit.yml)\n  ✗ no write, no secrets              ✓ contents: write\n  ✓ executes PR code                  ✗ never executes PR code\n  → uploads patch artifact             → applies patch, commits, pushes\n```\n\n## What issue does this PR close?\n\nN/A\n\n## How are these changes tested?\n\nUnfortunately, need to test it on the next set of Go Renovate PRs\n\n## Are there any user-facing changes?\n\nNo",
          "timestamp": "2026-05-21T21:31:23Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/877834fbfb606a2f477e58b81b8f7b2653539df5"
        },
        "date": 1779423079493,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.03,
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
          "id": "32abb25dd613ea36a97504ba79da5e427a3bef72",
          "message": "Add AaronRM as Triager (#3063)\n\n# Change Summary\n\nUpdate docs\n\n## What issue does this PR close?\n\n* Closes #3062",
          "timestamp": "2026-05-22T15:56:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/32abb25dd613ea36a97504ba79da5e427a3bef72"
        },
        "date": 1779508091676,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.03,
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
          "id": "32abb25dd613ea36a97504ba79da5e427a3bef72",
          "message": "Add AaronRM as Triager (#3063)\n\n# Change Summary\n\nUpdate docs\n\n## What issue does this PR close?\n\n* Closes #3062",
          "timestamp": "2026-05-22T15:56:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/32abb25dd613ea36a97504ba79da5e427a3bef72"
        },
        "date": 1779595779939,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.03,
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
          "id": "dae17d6eedbb715a81942cb8616a761ec45eeae3",
          "message": "perf: Box inner raw_batch_store record batch slices (#3077)\n\n# Change Summary\n\nBoxes raw_batch_store record batch slices. Attached issue explains the\nrationale, but basically, we have very a very large enum variant for\notap metrics (almost 1k) which is penalizing a lot of data structures.\n\nThis penalty is more visible when queues are larger and/or saturated,\nwhen batches are smaller in size, or when there are many processing\nstages.\n\n## What issue does this PR close?\n\n* Closes #3076\n\n## How are these changes tested?\n\nAd-hoc perf testing and also the manual pipelineperf run for the\nstandard continuous bench set.\n\n## Are there any user-facing changes?\n\nNo.",
          "timestamp": "2026-05-24T17:52:44Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/dae17d6eedbb715a81942cb8616a761ec45eeae3"
        },
        "date": 1779682837265,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.91,
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
          "id": "b163b4888f8faef824a428dae32f4c82c041048b",
          "message": "[Attributes Processor] Add attribute update action (#3084)\n\n# Change Summary\n\nAdds support for the attributes processor `update` action.\n\n`update` replaces existing attribute values for matching keys without\ninserting missing attributes. This provides the generic primitive needed\nfor redaction-style replacements while preserving existing-only\nsemantics.\n\nThe implementation reuses the existing attribute mutation path and\nhandles transport-optimized attribute batches by materializing\n`parent_id` before value changes that can alter equality runs.\n\n\n## What issue does this PR close?\n\n* Closes #3054 \n\n## How are these changes tested?\n\n - `cargo +1.95 fmt --all`\n  - `cargo +1.95 check -p otap-df-pdata`\n\n## Are there any user-facing changes?\n\nYes. Users can configure a new attributes processor `update` action to\nreplace existing attribute values for matching keys without inserting\nmissing attributes.\n\n```yaml\nprocessors:\n  attributes/update:\n    actions:\n      - action: update\n         key: secret\n         value: \"[MASKED]\"\n```\n\n---------\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-05-25T20:20:10Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b163b4888f8faef824a428dae32f4c82c041048b"
        },
        "date": 1779768352244,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.03,
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
          "id": "7aa2b45ef3cab3f6c482b46b19bfecb63ee4121d",
          "message": "task(comparison_dashboard): Update OTC sending queue default settings (#3089)\n\n# Change Summary\n\nUpdates sending queue settings for OTC to make for a fairer comparison.\n\n## What issue does this PR close?\n\n* Closes #3088\n\n## How are these changes tested?\n\nInitial results show a possible slight improvement which is interesting.\n\n| Suite | Rate | Mode | Produced | Received | Drop % | CPU avg % | RAM\nmax (MiB) |\n\n|----------|------|-----------|---------:|---------:|--------:|----------:|--------------:|\n| otlp | 200k | queue-cfg | 202,290 | 197,103 | 2.56 | 26.9 | 54 |\n| otlp | 200k | baseline | 201,901 | 198,794 | 2.56 | 26.9 | 60 |\n| otap | 200k | queue-cfg | 197,579 | 198,706 | -0.05 | 95.8 | 148 |\n| otap | 200k | baseline | 187,793 | 177,178 | 7.11 | 100.2 | 137 |\n| otlphttp | 200k | queue-cfg | 203,055 | 198,945 | 2.56 | 64.0 | 68 |\n| otlphttp | 200k | baseline | 207,940 | 196,510 | 5.00 | 76.1 | 80 |\n\n## Are there any user-facing changes?\n\nNo.",
          "timestamp": "2026-05-26T22:30:45Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7aa2b45ef3cab3f6c482b46b19bfecb63ee4121d"
        },
        "date": 1779855610205,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.09,
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
          "id": "7e9b8f342c6717d08f85d0fc9ab3275f595bdac1",
          "message": "fix(comparison_dashboard): Fix landing page backpressure detection for a comparison (#3116)\n\n# Change Summary\n\nPull out backpressure detection for a comparison to a helper - There was\nalready a helper for backpressure detection for a test, but not for an\nentire comparison which determines when the warning sign is displayed in\nthe legend.\n\n## What issue does this PR close?\n\n* Closes #3109\n\n## How are these changes tested?\n\n<img width=\"2333\" height=\"712\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/03a1335d-3d13-4275-b08a-0f299ee703d5\"\n/>\n\n## Are there any user-facing changes?\n\nNo",
          "timestamp": "2026-05-27T21:18:42Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7e9b8f342c6717d08f85d0fc9ab3275f595bdac1"
        },
        "date": 1779941324551,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.09,
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
          "id": "7d0af33f05ddaf772194302fa68db3f5b9100c64",
          "message": "chore(deps): update one_collect digest to f655a30 (#3130)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| one_collect | workspace.dependencies | digest | `293b7d3` → `f655a30`\n|\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - At any time (no schedule defined)\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4xOTguMCIsInVwZGF0ZWRJblZlciI6IjQzLjE5OC4wIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-28T23:19:33Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7d0af33f05ddaf772194302fa68db3f5b9100c64"
        },
        "date": 1780027830257,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.34,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.97,
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
          "id": "ddeb7d3c651e6fdb1abda29a396062b41b32389c",
          "message": "feat(host metrics receiver) - Add opt-in host load average metrics (#3141)\n\n# Change Summary\n\nAdds opt-in Linux load average metrics to the Rust `host_metrics`\nreceiver.\n\nWhen `families.load.enabled: true`, the receiver reads `/proc/loadavg`\nthrough the existing `host_view.root_path` abstraction and emits the\nCollector-compatible gauges:\n\n  - `system.cpu.load_average.1m`\n  - `system.cpu.load_average.5m`\n  - `system.cpu.load_average.15m`\n\nThe load family defaults to disabled because these metric names are\ndevelopment/experimental and are not registered in Semantic Conventions\n1.41.0.\n\n  ## What issue does this PR close?\n\n  * Closes #3067\n\n  ## How are these changes tested?\n\n- `cargo check -p otap-df-core-nodes --features otap-df-otap/crypto-ring\n--all-targets`\n- `cargo test -p otap-df-core-nodes --features otap-df-otap/crypto-ring\nhost_metrics_receiver` on Linux test VM\n  - `cargo xtask check` on Linux test VM\n- `npx markdownlint-cli\nrust/otap-dataflow/crates/core-nodes/src/receivers/host_metrics_receiver/README.md`\n  - `python3 tools/sanitycheck.py`\n\n  ## Are there any user-facing changes?\n\n  Yes. Users can now enable Linux load average metrics with:\n\n  ```yaml\n  families:\n    load:\n      enabled: true\n      interval: 30s\n```\n\n  ### Changelog\n\n  - [x] Added a .chloggen/*.yaml entry, OR this PR is a chore (indicated in title).",
          "timestamp": "2026-05-29T23:21:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ddeb7d3c651e6fdb1abda29a396062b41b32389c"
        },
        "date": 1780113359359,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.97,
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
          "id": "ddeb7d3c651e6fdb1abda29a396062b41b32389c",
          "message": "feat(host metrics receiver) - Add opt-in host load average metrics (#3141)\n\n# Change Summary\n\nAdds opt-in Linux load average metrics to the Rust `host_metrics`\nreceiver.\n\nWhen `families.load.enabled: true`, the receiver reads `/proc/loadavg`\nthrough the existing `host_view.root_path` abstraction and emits the\nCollector-compatible gauges:\n\n  - `system.cpu.load_average.1m`\n  - `system.cpu.load_average.5m`\n  - `system.cpu.load_average.15m`\n\nThe load family defaults to disabled because these metric names are\ndevelopment/experimental and are not registered in Semantic Conventions\n1.41.0.\n\n  ## What issue does this PR close?\n\n  * Closes #3067\n\n  ## How are these changes tested?\n\n- `cargo check -p otap-df-core-nodes --features otap-df-otap/crypto-ring\n--all-targets`\n- `cargo test -p otap-df-core-nodes --features otap-df-otap/crypto-ring\nhost_metrics_receiver` on Linux test VM\n  - `cargo xtask check` on Linux test VM\n- `npx markdownlint-cli\nrust/otap-dataflow/crates/core-nodes/src/receivers/host_metrics_receiver/README.md`\n  - `python3 tools/sanitycheck.py`\n\n  ## Are there any user-facing changes?\n\n  Yes. Users can now enable Linux load average metrics with:\n\n  ```yaml\n  families:\n    load:\n      enabled: true\n      interval: 30s\n```\n\n  ### Changelog\n\n  - [x] Added a .chloggen/*.yaml entry, OR this PR is a chore (indicated in title).",
          "timestamp": "2026-05-29T23:21:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ddeb7d3c651e6fdb1abda29a396062b41b32389c"
        },
        "date": 1780201333847,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.97,
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
          "id": "ddeb7d3c651e6fdb1abda29a396062b41b32389c",
          "message": "feat(host metrics receiver) - Add opt-in host load average metrics (#3141)\n\n# Change Summary\n\nAdds opt-in Linux load average metrics to the Rust `host_metrics`\nreceiver.\n\nWhen `families.load.enabled: true`, the receiver reads `/proc/loadavg`\nthrough the existing `host_view.root_path` abstraction and emits the\nCollector-compatible gauges:\n\n  - `system.cpu.load_average.1m`\n  - `system.cpu.load_average.5m`\n  - `system.cpu.load_average.15m`\n\nThe load family defaults to disabled because these metric names are\ndevelopment/experimental and are not registered in Semantic Conventions\n1.41.0.\n\n  ## What issue does this PR close?\n\n  * Closes #3067\n\n  ## How are these changes tested?\n\n- `cargo check -p otap-df-core-nodes --features otap-df-otap/crypto-ring\n--all-targets`\n- `cargo test -p otap-df-core-nodes --features otap-df-otap/crypto-ring\nhost_metrics_receiver` on Linux test VM\n  - `cargo xtask check` on Linux test VM\n- `npx markdownlint-cli\nrust/otap-dataflow/crates/core-nodes/src/receivers/host_metrics_receiver/README.md`\n  - `python3 tools/sanitycheck.py`\n\n  ## Are there any user-facing changes?\n\n  Yes. Users can now enable Linux load average metrics with:\n\n  ```yaml\n  families:\n    load:\n      enabled: true\n      interval: 30s\n```\n\n  ### Changelog\n\n  - [x] Added a .chloggen/*.yaml entry, OR this PR is a chore (indicated in title).",
          "timestamp": "2026-05-29T23:21:41Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ddeb7d3c651e6fdb1abda29a396062b41b32389c"
        },
        "date": 1780288610775,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.97,
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
          "id": "b31a3c75011eccdc455a74fcc4a9838eefc5a6da",
          "message": "fix(deps): update module go.opentelemetry.io/collector/pdata to v1.59.0 (#3162)\n\nThis PR contains the following updates:\n\n| Package | Change |\n[Age](https://docs.renovatebot.com/merge-confidence/) |\n[Confidence](https://docs.renovatebot.com/merge-confidence/) |\n|---|---|---|---|\n|\n[go.opentelemetry.io/collector/pdata](https://redirect.github.com/open-telemetry/opentelemetry-collector)\n| `v1.58.0` → `v1.59.0` |\n![age](https://developer.mend.io/api/mc/badges/age/go/go.opentelemetry.io%2fcollector%2fpdata/v1.59.0?slim=true)\n|\n![confidence](https://developer.mend.io/api/mc/badges/confidence/go/go.opentelemetry.io%2fcollector%2fpdata/v1.58.0/v1.59.0?slim=true)\n|\n\n---\n\n### Release Notes\n\n<details>\n<summary>open-telemetry/opentelemetry-collector\n(go.opentelemetry.io/collector/pdata)</summary>\n\n###\n[`v1.59.0`](https://redirect.github.com/open-telemetry/opentelemetry-collector/blob/HEAD/CHANGELOG.md#v1590v01530)\n\n##### 🛑 Breaking changes 🛑\n\n- `pkg/configoptional`: Stabilize feature gate\nconfigoptional.AddEnabledField\n([#&#8203;15333](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15333))\n- `pkg/confmap`: Stabilize confmap.newExpandedValueSanitizer feature\ngate\n([#&#8203;15339](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15339))\n- `pkg/exporterhelper`: mark exporter.PersistRequestContext as stable\n([#&#8203;15330](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15330))\n- `pkg/otelcol`: Stabilize otelcol.printInitialConfig gate\n([#&#8203;15340](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15340))\n- `pkg/pdata`: Remove pdata.useCustomProtoEncoding feature gate\n([#&#8203;15332](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15332))\n- `pkg/service`: Stabilize telemetry.UseLocalHostAsDefaultMetricsAddress\ngate\n([#&#8203;15342](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15342))\n- `pkg/xpdata`: Stabilize pdata.enableRefCounting feature gate\n([#&#8203;15331](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15331))\n\n##### 🧰 Bug fixes 🧰\n\n- `pkg/config/configgrpc`: Fix memory corruption and fatal error in\nSnappy\n([#&#8203;15237](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15237),\n[#&#8203;15320](https://redirect.github.com/open-telemetry/opentelemetry-collector/issues/15320))\n\n<!-- previous-version -->\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on Monday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4yMDYuMSIsInVwZGF0ZWRJblZlciI6IjQzLjIwNi4xIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\n---------\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>\nCo-authored-by: otelbot <197425009+otelbot@users.noreply.github.com>",
          "timestamp": "2026-06-01T23:18:45Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b31a3c75011eccdc455a74fcc4a9838eefc5a6da"
        },
        "date": 1780374557605,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.32,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.97,
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
          "id": "9fb01d6d441d84fcdb5a0a75d377f041fa0cdfca",
          "message": "Add uncompressed bytes-per-log metric to traffic generator benchmarks (#3026)\n\nCloses #2987\n\nAdds a `logs_bytes_produced` counter metric to the traffic generator\nreceiver that tracks the total protobuf-encoded (uncompressed) bytes of\nlog payloads produced. The benchmark report SQL then computes\n`uncompressed_bytes_per_log` from this counter, enabling direct\ncomparison of uncompressed payload size against the egress (compressed)\nbytes per log.\n\n### Changes\n- **metrics.rs**: Added `logs_bytes_produced: Counter<u64>` with unit\n`By`\n- **mod.rs**: Record payload bytes in `export_pdata()` for log signals\n(captured before ownership move)\n- **integration_report_logs.yaml** & **report_logs.yaml**: Added\n`logs_bytes_produced` to metric filter and `uncompressed_bytes_per_log`\ncomputed metric to report SQL",
          "timestamp": "2026-06-02T22:17:51Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9fb01d6d441d84fcdb5a0a75d377f041fa0cdfca"
        },
        "date": 1780461278443,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-arm64-binary-size",
            "value": 98.03,
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
          "id": "3a482a342201f71bae5e100998c2c16d05ac0f9d",
          "message": "chore(deps): update all patch versions to v1.26.4 (#3185)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| [go](https://go.dev/)\n([source](https://redirect.github.com/golang/go)) | toolchain | patch |\n`1.26.3` → `1.26.4` |\n\n---\n\n### Release Notes\n\n<details>\n<summary>golang/go (go)</summary>\n\n###\n[`v1.26.4`](https://redirect.github.com/golang/go/compare/go1.26.3...go1.26.4)\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am every weekday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4yMDkuMCIsInVwZGF0ZWRJblZlciI6IjQzLjIwOS40IiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-06-03T18:52:43Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/3a482a342201f71bae5e100998c2c16d05ac0f9d"
        },
        "date": 1780547587428,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.38,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.03,
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
          "id": "c6ed105cab28e537bf5c2c81a97e9b63677d3cff",
          "message": "fix(comparison_dashboard): Remove fluent bit otlpgrpc gzip/zstd suites (#3200)\n\n# Change Summary\n\nFluent bit seems to ignore compression settings on the otlp grpc path,\nso removing those suites.",
          "timestamp": "2026-06-04T03:45:28Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c6ed105cab28e537bf5c2c81a97e9b63677d3cff"
        },
        "date": 1780633063132,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 97.97,
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
          "id": "f9b6074475f5c7916343ca39bf98b2879f053775",
          "message": "fix: replace OPL `exclude`/`date_time` keywords to match spec (#3180)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nIn https://github.com/open-telemetry/otel-arrow/pull/3051 we are adding\na specification for OPL. It has some minor keyword differences from the\ncurrent implementation.\n\nThe keyword for the operation it specifies to remove attributes is\n`remove`, but currently we use `exclude`. I think `remove` is a more\nsensible name, so we'll make this change. (`project-away` will remain an\nalias`).\n\n```\n// before this would be `exclude attributes[\"x\"]`\nlogs | remove attributes[\"x\"]\n```\n\nThe tag we use for timestamp literals also changes to match the spec,\nfrom `date_time` to `timestamp`. This seems more sensible as well,\nbecause that is what the arrow type is called. E.g., now\ntimestamp/datetime literals will be defined like `timestamp\"...\"`\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Relates to #3051 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nYes - this is a breaking change to OPL syntax\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-06T00:22:55Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f9b6074475f5c7916343ca39bf98b2879f053775"
        },
        "date": 1780718429056,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.22,
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
          "id": "f9b6074475f5c7916343ca39bf98b2879f053775",
          "message": "fix: replace OPL `exclude`/`date_time` keywords to match spec (#3180)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nIn https://github.com/open-telemetry/otel-arrow/pull/3051 we are adding\na specification for OPL. It has some minor keyword differences from the\ncurrent implementation.\n\nThe keyword for the operation it specifies to remove attributes is\n`remove`, but currently we use `exclude`. I think `remove` is a more\nsensible name, so we'll make this change. (`project-away` will remain an\nalias`).\n\n```\n// before this would be `exclude attributes[\"x\"]`\nlogs | remove attributes[\"x\"]\n```\n\nThe tag we use for timestamp literals also changes to match the spec,\nfrom `date_time` to `timestamp`. This seems more sensible as well,\nbecause that is what the arrow type is called. E.g., now\ntimestamp/datetime literals will be defined like `timestamp\"...\"`\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Relates to #3051 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nYes - this is a breaking change to OPL syntax\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-06T00:22:55Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f9b6074475f5c7916343ca39bf98b2879f053775"
        },
        "date": 1780806461672,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.22,
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
          "id": "f9b6074475f5c7916343ca39bf98b2879f053775",
          "message": "fix: replace OPL `exclude`/`date_time` keywords to match spec (#3180)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nIn https://github.com/open-telemetry/otel-arrow/pull/3051 we are adding\na specification for OPL. It has some minor keyword differences from the\ncurrent implementation.\n\nThe keyword for the operation it specifies to remove attributes is\n`remove`, but currently we use `exclude`. I think `remove` is a more\nsensible name, so we'll make this change. (`project-away` will remain an\nalias`).\n\n```\n// before this would be `exclude attributes[\"x\"]`\nlogs | remove attributes[\"x\"]\n```\n\nThe tag we use for timestamp literals also changes to match the spec,\nfrom `date_time` to `timestamp`. This seems more sensible as well,\nbecause that is what the arrow type is called. E.g., now\ntimestamp/datetime literals will be defined like `timestamp\"...\"`\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Relates to #3051 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nYes - this is a breaking change to OPL syntax\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-06T00:22:55Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f9b6074475f5c7916343ca39bf98b2879f053775"
        },
        "date": 1780893114921,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.16,
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
          "id": "3c08bc543efb9aed5dcfaaf6a3c1403f00b1c0a9",
          "message": "chore: update jemalloc to 0.7 (#3247)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\nupdate jemalloc to 0.7\n\nfixes/sueprsedes https://github.com/open-telemetry/otel-arrow/pull/3238\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Closes #NNN\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-08T22:16:21Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/3c08bc543efb9aed5dcfaaf6a3c1403f00b1c0a9"
        },
        "date": 1780977553124,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.34,
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
          "id": "cdaf6e876f6bbede75a07716197e0b03c4b848a8",
          "message": "docs: document dataflow core node configuration (#3213)\n\n**Not ready for review yet**\n\n# Change Summary\n\nDocument the Arrow Dataflow Engine configuration model and core-node\ncatalog. This adds a single discovery path for users, creates per-node\nREADME files next to core-node implementations, and standardizes node\ndocumentation around metadata, configuration, examples, telemetry,\nlimits, stability, and related docs.\n\n## What issue does this PR close?\n\n* Closes #3212\n\n## How are these changes tested?\n\n* `npx markdownlint-cli2 ...`\n* `python3 tools/sanitycheck.py`\n* `cargo xtask check`\n\n## Are there any user-facing changes?\n\nYes. This PR adds user-facing documentation for Arrow Dataflow Engine\nconfiguration and core nodes.\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).\n\n---------\n\nCo-authored-by: Cijo Thomas <cithomas@microsoft.com>\nCo-authored-by: albertlockett <a.lockett@f5.com>\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>",
          "timestamp": "2026-06-09T23:39:05Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/cdaf6e876f6bbede75a07716197e0b03c4b848a8"
        },
        "date": 1781064914310,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 110.86,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.47,
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
          "id": "08d1c48b986a0c52242901a2bcb8cfcfb46d00bb",
          "message": "[Journald receiver] Implement journald ingestion (#3134)\n\n# Change Summary\n\nAdds the Rust `receiver:journald` implementation for Linux\n`systemd-journald` ingestion.\n\nThis includes `sd-journal` based log reading, batching, OTAP Arrow log\nprojection, Ack/Nack-driven checkpoint advancement, durable cursor\ncheckpointing, extraction limits, receiver metrics, documentation, and\nchangelog entry.\n\nThe README and design doc document current operational limits, including\none active owner per host journal source/checkpoint identity,\nprocess-local duplicate protection only, deferred cross-process locking,\nand deferred production hardening for partial journal access,\nfirst-checkpoint anchoring, and aggregate batch byte limits.\n\n  ## What issue does this PR close?\n\n  * Closes #2858 \n\n  ## How are these changes tested?\n\n* Added unit tests for config validation, checkpoint read/write, corrupt\ncheckpoint handling, log projection, extraction limits, lease behavior,\nand Ack/Nack/drain state handling.\n  * Ran targeted Rust checks locally:\n* `cargo check -p otap-df-core-nodes --features\notap-df-otap/crypto-ring`\n* `cargo test -p otap-df-core-nodes --features otap-df-otap/crypto-ring\njournald_receiver`\n* `cargo clippy -p otap-df-core-nodes --features\notap-df-otap/crypto-ring --all-targets -- -D warnings`\n  * Ran markdown and sanity checks for docs/changelog updates.\n* Manually validated on an Ubuntu VM using real `systemd-journald` input\nthrough the `df_engine` pipeline to local debug/noop output. Covered\nbasic ingestion, unit filtering, stdout/stderr, metadata mapping, start\npositions, checkpoint restart behavior, checkpoint deletion behavior,\ninvalid config, missing permissions smoke, lifecycle shutdown, and high\nvolume smoke.\n\n  ## Are there any user-facing changes?\n\nYes. This adds a Linux-only `receiver:journald` component for reading\nlogs from `systemd-journald`.\n\n  ### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).\n\n---------\n\nCo-authored-by: Joshua MacDonald <jmacd@users.noreply.github.com>",
          "timestamp": "2026-06-10T23:17:31Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/08d1c48b986a0c52242901a2bcb8cfcfb46d00bb"
        },
        "date": 1781152110674,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.14,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.72,
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
          "id": "877dd362a5547ce59c0243a1f0203cc85cd3d892",
          "message": "feat(engine): Add extension lifecycle monitor + metrics, decouple from PipelineContext (#3143)\n\n## Change Summary\n\nAdds per-extension lifecycle telemetry and decouples the extension\nsubsystem\nfrom `PipelineContext` so extensions can be hosted at any scope.\n\n- **Scope-agnostic extensions.** New `ExtensionContext` carrying only\n  `ControllerContext`; `ExtensionFactory::create` no longer takes\n`PipelineContext`. `broadcast_shutdown` now takes a caller-supplied\nreason.\n- **Per-variant telemetry.** `ExtensionAttributeSet` gains\n`extension_variant` (`local` | `shared`); dual-variant extensions\nregister\ntwo distinct entities with independent `EntityTelemetryGuard` ownership\nso\n  dropping one variant only tears down its own entity tree.\n- **Per-scope attribution.** `ExtensionAttributeSet` carries a nested\n`ExtensionScopeAttributeSet` (today: `pipeline(group, pipeline_id, core,\nnum_cores)`) so the same extension `id+variant` hosted at different\nscopes\nmints distinct `EntityKey`s. Same shape will extend cleanly to\nengine/group\n  scopes.\n- **Channel attribute split.** `ChannelAttributeSet` split into\n`NodeChannelAttributeSet` and `ExtensionChannelAttributeSet`; fixes a\nleak\nwhere extension channel `MetricSet` keys were never tracked via the\nentity\n  handle.\n- **`ExtensionMetricsMonitor`.** New monitor wired into the lifecycle\npump\n  emits counters (`spawned` / `completed` / `shutdown_sent` / errors /\n  timeouts) and a state gauge per extension entity. Hardening:\n- `register()` is the single source of `spawned` credit — it\nsynchronously\nsets `state = Spawned` and increments the `spawned` counter, eliminating\n    a class of undercount bugs by construction (no transient pre-Spawned\n    state to drop events against).\n- `wait_all_spawned()` is preserved as a pure scheduling barrier so node\n    tasks cannot observe partially-initialized extensions.\n  - Biased `select!` ordering (started → completions → ticks); sticky-\n    terminal monotonic state machine; `mark_stragglers_as_timeout`\n    reconciles non-terminal entries at shutdown; `Drop`-time registry\n    unregister. Each scope owns its own monitor + lifecycle so ticks and\n    shutdowns never cross scopes.\n  - State gauge discriminants pinned (`Spawned=1, ShutdownSent=2,\nCompletedOk=3, Failed=4, TimedOut=5`) with `0` reserved as vacant for\n    dashboard compatibility; enforced by an exhaustive-match test that\n    fails to compile if a new variant is added without updating the\n    contract.\n\n## What issue does this PR close?\n\nCloses \"Per-extension lifecycle metrics.\" on Extension lifecycle\nfollow-ups\n#3039.\n\n## How are these changes tested?\n\n- ~30+ new unit tests across `extension/tests.rs`,\n`extension_lifecycle.rs`,\nand `extension_monitor.rs` covering per-variant isolation,\n`CollectTelemetry`\nround-trip and interval gating, `mark_stragglers_as_timeout`\nreconciliation,\nsticky-terminal `Completed`, state-gauge refresh, `Drop`-time\nunregister,\ncross-scope isolation (distinct scopes — pipelines / cores / groups\ntoday —\n  for both shutdown broadcast and collect-telemetry fanout), live\n  two-pipelines-through-shared-reporter, panicking `CollectTelemetry`\n  handler, snapshot-after-`Drop` identity, `wire_telemetry` dual\n  register/release cycle, `broadcast_shutdown` idempotency, passive\nextensions excluded from monitor, state-gauge integer encoding\nstability,\n  `try_send` failure non-skew, panic-before-`notify_started` surfaced\n  through `wait_all_spawned`, and structural invariants ensuring\n  `ShutdownSent` and fast completions never undercount `spawned`.\n- Existing extension lifecycle and `extension_e2e` integration tests\n  updated and passing.\n- `cargo test -p otap-df-engine` green (446 lib + 28 e2e).\n- `cargo fmt --all` and `cargo clippy -p otap-df-engine --all-targets\n  -- -D warnings` clean.\n\n## Are there any user-facing changes?\n\n**Breaking** (out-of-tree consumers will get compile errors):\n\n- `ExtensionFactory::create` signature changed: no longer receives\n  `PipelineContext`; it now receives `ExtensionContext`. Out-of-tree\n  extensions need to be updated.\n- `PipelineContext::register_channel_entity` renamed to\n  `register_node_channel_entity`; `channel_attribute_set` renamed to\n`node_channel_attribute_set`. The `ChannelAttributeSet` struct is split\n  into `NodeChannelAttributeSet` and `ExtensionChannelAttributeSet`.\n\n**New telemetry surface:** `extension.lifecycle.*` metrics with\n`extension_id` + `extension_variant` attributes.\n\n**No config or wire-format changes.**\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-06-11T23:18:24Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/877dd362a5547ce59c0243a1f0203cc85cd3d892"
        },
        "date": 1781238648985,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.37,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.72,
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
          "id": "591a9c12c0b36025f9f7a899eb840d4fee913ffd",
          "message": "feat: add controller extension hooks (#3281)\n\n# Change Summary\n\nAdds a controller extension hooks for trusted in-process integrations.\n\nThis PR introduces:\n- an `engine.extensions` configuration section keyed by extension ID\n- a reusable `ExtensionUserConfig` envelope for engine-level extension\ndeclarations\n- `ControllerExtensionRegistry` and `ControllerRunOptions` for embedding\napplications to register extension factories\n- `ControllerExtensionContext`, giving extensions access to:\n    - their extension ID and config\n    - the semantic control-plane handle\n    - observed runtime state\n    - the shared telemetry registry\n    - the initial engine config\n- extension lifecycle startup/shutdown handling on dedicated controller\nextension tasks\n\nThe default behavior remains unchanged: if no engine extensions are\nconfigured, no extension tasks are started. If an extension is\nconfigured without a registered factory, controller startup fails with a\nclear\nerror.\n\n## What issue does this PR close?\n\nCloses https://github.com/open-telemetry/otel-arrow/issues/3263\n\n## How are these changes tested?\n\nTested with:\n\n- `cargo xtask check`\n\n## Are there any user-facing changes?\n\nYes. This adds a new public engine configuration field,\n`engine.extensions`, and new controller APIs for embedding applications\nthat need to register trusted in-process controller extensions.\n\nExisting configurations remain backward compatible because\n`engine.extensions` defaults to empty.\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).\n\n---------\n\nCo-authored-by: Cijo Thomas <cithomas@microsoft.com>",
          "timestamp": "2026-06-13T00:11:46Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/591a9c12c0b36025f9f7a899eb840d4fee913ffd"
        },
        "date": 1781324261094,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
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
          "id": "f8cd17f084c1a766f887530531ad06f546080c90",
          "message": "chore: rename traffic generator example configs (#3288)\n\nRenames the traffic generator example config files under\n`rust/otap-dataflow/configs` from the old `fake-*` prefix to\n`trafficgen-*`, and updates the docs/examples that reference them.\n\nFollow-up to #2891.\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-06-13T01:12:36Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f8cd17f084c1a766f887530531ad06f546080c90"
        },
        "date": 1781411757506,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
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
          "id": "f8cd17f084c1a766f887530531ad06f546080c90",
          "message": "chore: rename traffic generator example configs (#3288)\n\nRenames the traffic generator example config files under\n`rust/otap-dataflow/configs` from the old `fake-*` prefix to\n`trafficgen-*`, and updates the docs/examples that reference them.\n\nFollow-up to #2891.\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-06-13T01:12:36Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f8cd17f084c1a766f887530531ad06f546080c90"
        },
        "date": 1781499610347,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
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
          "id": "5b0ef7b8f627d6bd258b5990984799257f0cee51",
          "message": "feat(Linux user_events receiver) - Add per-subscription pending governance  (#3153)\n\n# Change Summary\n\nAdds Phase 1 per-subscription pending governance for the Linux\n`user_events` receiver.\n\nThis introduces optional per-subscription pending limits:\n\n  - `subscriptions[].limits.max_pending_events`\n  - `subscriptions[].limits.max_pending_bytes`\n\nWhen no effective subscription limits are configured, the receiver keeps\nthe existing shared FIFO pending queue behavior. When any subscription\nhas effective limits, the receiver uses per-subscription pending queues\nwith round-robin drain under the existing global\n`session.max_pending_events` and `session.max_pending_bytes` ceilings.\n\nAdmission is enforced in the `one_collect` callback before\n`event_data().to_vec()`, so rejected samples avoid payload heap\nallocation.\n\nThis improves receiver-side fairness for pending queue capacity and\ndrain order. It does not provide kernel-ring isolation: perf-ring loss\nand one_collect parse work remain shared by tracepoints in the same\nreceiver/session.\n\n## What issue does this PR close?\n\n  * Addresses receiver-side pending governance for #3071\n\n## How are these changes tested?\n\n  - `cargo fmt --all`\n  - `git diff --check`\n- `cargo check -p otap-df-contrib-nodes --features\nuser_events-receiver,otap-df-otap/crypto-ring --all-targets`\n- `cargo clippy -p otap-df-contrib-nodes --features\nuser_events-receiver,otap-df-otap/crypto-ring --all-targets -- -D\nwarnings`\n  - Linux VM:\n    - user_events adapter tests: 13 passed\n    - user_events config validation subset: 6 passed\n- manual noisy/quiet tracepoint test confirmed quiet records pass with\nnoisy tracepoint pending limits enabled\n\n## Are there any user-facing changes?\n\nYes. The Linux `user_events` receiver now supports optional\nper-subscription pending limits:\n\n  ```yaml\n  subscriptions:\n    - tracepoint: \"user_events:noisy_tracepoint\"\n      format:\n        type: tracefs\n      limits:\n        max_pending_events: 1024\n        max_pending_bytes: 4194304\n```\n\nWhen subscriptions[].limits is present, at least one pending cap must be set and configured cap values must be greater than zero.\n\nPhase 1 keeps dropped_pending_overflow aggregate-only. Per-tracepoint/per-cap drop attribution is left for a follow-up.\n\n### Changelog\n\n  * [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore` (indicated in title).",
          "timestamp": "2026-06-16T00:11:45Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5b0ef7b8f627d6bd258b5990984799257f0cee51"
        },
        "date": 1781585013072,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.54,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
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
          "id": "f7ff5155cea816289b8916cb0035e349ac7263fb",
          "message": "chore(deps): update geneva-uploader digest to 97c02a7 (#3308)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| geneva-uploader | workspace.dependencies | digest | `e263bdb` →\n`97c02a7` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on Monday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4yMTkuMCIsInVwZGF0ZWRJblZlciI6IjQzLjIxOS4wIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-06-16T23:34:58Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f7ff5155cea816289b8916cb0035e349ac7263fb"
        },
        "date": 1781671104660,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.1,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.47,
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
          "id": "105dc7add1ae55f3412e439815c5df6fdfc07ee4",
          "message": "chore(deps): update all patch versions to >=0.3.47, <0.3.50 (#3301)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| [time](https://crates.io/crates/time) | workspace.dependencies | patch\n| `>=0.3.47, <0.3.48` → `>=0.3.47, <0.3.50` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am every weekday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4yMTkuMCIsInVwZGF0ZWRJblZlciI6IjQzLjIxOS4wIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-06-17T17:01:21Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/105dc7add1ae55f3412e439815c5df6fdfc07ee4"
        },
        "date": 1781757028408,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.12,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.47,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Swapnil Ashtekar",
            "username": "swashtek",
            "email": "46826200+swashtek@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "bccc890d6a977f683edfcfbf92439f2b0e8ba822",
          "message": "chore: align Bool32 decoding in tests with one_collect's Win32 BOOL fix (#3319)\n\n# Change Summary\nchore: align Bool32 decoding with one_collect's Win32 BOOL fix; update\ntests and documentation\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Closes #NNN\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\nNo\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-18T23:22:05Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/bccc890d6a977f683edfcfbf92439f2b0e8ba822"
        },
        "date": 1781845140180,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.41,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "678b5d63f2cf952d2418827f552d6501c2d5d188",
          "message": "OTAP exporter: support static request headers natively (#3315)\n\nCloses #3314.\n\n## What\n\nThe native OTAP (Arrow-protocol) exporter (`urn:otel:exporter:otap`) now\nsupports static request `headers`, attaching them as the **initial\nmetadata of each Arrow log/metric/trace stream**. The previous\nconfig-load rejection (`reject_unsupported_headers`) is removed.\n\n## Approach\n\n- **Shared builder at the `GrpcClientSettings` layer** (the maintainer's\npreferred option from the #3303 review thread): added\n`build_static_metadata(&HashMap) -> Option<MetadataMap>` as a single\nsource of truth, and refactored the OTLP/gRPC exporter to delegate to it\n(removing its private duplicate).\n- **Attach once per stream (re)open**: in `stream_arrow_batches`, the\npre-built `Option<Arc<MetadataMap>>` template is cloned onto the\nstream-open `Request` via `into_streaming_request()` + `metadata_mut()`.\nThe `StreamingArrowService` trait and its impls are untouched.\n- **Hot path preserved**: `create_req_stream` (the\nper-`BatchArrowRecords` path) is unchanged; the only added cost is one\n`MetadataMap` clone per stream establishment (documented in the README\n\"Limits\").\n- **Validation reused**: header validation stays in\n`GrpcClientSettings::validate()` (ASCII, reserved gRPC metadata,\ncase-insensitive duplicates), now also invoked from `from_config` as\ndefense-in-depth, and skipped/invalid entries are logged.\n\n## Tests\n\n- Removed the 2 rejection tests; added positive config tests (accept\nvalid headers; reject reserved) plus a `from_config` rejection test.\n- End-to-end: configured headers are present as **initial stream\nmetadata on logs, metrics, AND traces**; multiple headers; empty/absent\nmap sends no metadata.\n- Deterministic unit test that headers are re-applied on **every**\nstream (re)open.\n- Builder unit tests (empty → `None`, builds map, skips invalid).\n- `cargo xtask check` passes (structure, `fmt`, `clippy -D warnings`,\nworkspace tests).\n\n## Notes\n\nComplements #3306: OTAP header **values** should later adopt the same\nopaque/secret type so credentials don't leak into telemetry.\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-06-19T18:24:55Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/678b5d63f2cf952d2418827f552d6501c2d5d188"
        },
        "date": 1781928803567,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.47,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "678b5d63f2cf952d2418827f552d6501c2d5d188",
          "message": "OTAP exporter: support static request headers natively (#3315)\n\nCloses #3314.\n\n## What\n\nThe native OTAP (Arrow-protocol) exporter (`urn:otel:exporter:otap`) now\nsupports static request `headers`, attaching them as the **initial\nmetadata of each Arrow log/metric/trace stream**. The previous\nconfig-load rejection (`reject_unsupported_headers`) is removed.\n\n## Approach\n\n- **Shared builder at the `GrpcClientSettings` layer** (the maintainer's\npreferred option from the #3303 review thread): added\n`build_static_metadata(&HashMap) -> Option<MetadataMap>` as a single\nsource of truth, and refactored the OTLP/gRPC exporter to delegate to it\n(removing its private duplicate).\n- **Attach once per stream (re)open**: in `stream_arrow_batches`, the\npre-built `Option<Arc<MetadataMap>>` template is cloned onto the\nstream-open `Request` via `into_streaming_request()` + `metadata_mut()`.\nThe `StreamingArrowService` trait and its impls are untouched.\n- **Hot path preserved**: `create_req_stream` (the\nper-`BatchArrowRecords` path) is unchanged; the only added cost is one\n`MetadataMap` clone per stream establishment (documented in the README\n\"Limits\").\n- **Validation reused**: header validation stays in\n`GrpcClientSettings::validate()` (ASCII, reserved gRPC metadata,\ncase-insensitive duplicates), now also invoked from `from_config` as\ndefense-in-depth, and skipped/invalid entries are logged.\n\n## Tests\n\n- Removed the 2 rejection tests; added positive config tests (accept\nvalid headers; reject reserved) plus a `from_config` rejection test.\n- End-to-end: configured headers are present as **initial stream\nmetadata on logs, metrics, AND traces**; multiple headers; empty/absent\nmap sends no metadata.\n- Deterministic unit test that headers are re-applied on **every**\nstream (re)open.\n- Builder unit tests (empty → `None`, builds map, skips invalid).\n- `cargo xtask check` passes (structure, `fmt`, `clippy -D warnings`,\nworkspace tests).\n\n## Notes\n\nComplements #3306: OTAP header **values** should later adopt the same\nopaque/secret type so credentials don't leak into telemetry.\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-06-19T18:24:55Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/678b5d63f2cf952d2418827f552d6501c2d5d188"
        },
        "date": 1782017017424,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.08,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.47,
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
          "id": "1ffc9f7dbd42043e9392173e273deb995f2d59b0",
          "message": "ci: bump weaver to v0.24.0 (#3324)\n\nMove the df-engine internal-observability workflow from weaver v0.23.0\n(binary) + unreleased composite-action SHA-pin (weaver#1448) to the\npublished v0.24.0 release.\n\nv0.24.0 is the first tag to ship the `weaver-live-check-start` /\n`weaver-live-check-stop` composite actions, so this replaces the\nprevious \"pinned to a merge commit on main\" workaround with a real tag\n(SHA-pinned to `9b84f5c`). The `WEAVER_VERSION` binary moves to v0.24.0\nin lockstep.\n\nAlso bumps `host-metrics-weaver-live-check` in `rust-ci.yml` to v0.24.0\nso both weaver-driven CI jobs stay on the same release. The Python\nsummary script in that job reads `signal_type` / `signal_name` from\n`all_advice[]` (PolicyFinding fields), unchanged in v0.24.0.\n\nv0.24.0 has breaking renames in the `--emit-otlp-logs` schema, but\nneither workflow consumes that surface — they parse the JSON report\n(`samples[].log.event_name`, `live_check_result.highest_advice_level`,\n`all_advice[].signal_type/signal_name`), all of which are unchanged.",
          "timestamp": "2026-06-21T21:07:31Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1ffc9f7dbd42043e9392173e273deb995f2d59b0"
        },
        "date": 1782103574448,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.08,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.53,
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
          "id": "42083ad132c2dce8958a0c6f39ede8431311dcd6",
          "message": "feat(engine/topic): foundation & primitives for broadcast all ack mode (PR 1/3) (#3323)\n\n> Copy of #3250, recreated after a bad rebase\n\n# Change Summary\n\nPart of #2252. First of three independently shippable PRs adding a\nbroadcast `all` ack mode for topic hops.\n\n## Why\n\nToday a broadcast topic acks upstream as soon as the **first**\nsubscriber acks. The target use case needs strict fan-out: ack upstream\nonly once **every** downstream pipeline acks. So we're adding an opt-in\n`ack_mode: all`. The modes differ fundamentally: `first` asks \"has\n*anyone* acked?\", while `all` asks \"have *all required* subscribers\nacked?\" — which means `all` must know the exact required set per\nmessage.\n\n## Scope: groundwork only, no behavior change\n\nLands the types and primitives (unit-tested) but **nothing honors `all`\nyet** — every caller passes `First` and the engine ignores it, so\nbehavior is identical to today. PR 2 wires the engine; PR 3 adds the\nconfig knob, validation, and docs.\n\n## What's included\n\n- **Mode enum** (`config/src/topic.rs`): `TopicBroadcastAckMode { First,\nAll }`, default `First`. The config field using it lands in PR 3.\n- **Consensus-aware tracker** (`engine/src/topic/types.rs`): the publish\ntracker can now hold the *set of subscribers still owing an ack* for a\nmessage, resolving Ack when it empties or Nack when any required\nsubscriber nacks/disappears. Tested, not yet called.\n- **Ring split** (`engine/src/topic/topic.rs`): `publish()` is now a\nwrapper over `reserve_seq()` (claim the sequence) + `commit_slot()`\n(write + wake). Called back-to-back, so behavior is unchanged; PR 2\nneeds them separate (below).\n- **Options field**: `ack_mode` threaded through all topic construction\nsites as `First`; the engine ignores it.\n\n## Why the ring is split\n\n`all` mode must record, at publish time, the required ack set for a\nmessage — and it must match who actually *receives* it (a subscriber\ngets message N only if it joined at/before N). If a publish and a new\nsubscription interleave wrong, a message can either hang forever waiting\non a subscriber that never got it (liveness bug) or silently drop one\nthat did (correctness bug). PR 2 fixes this by claiming the sequence and\nsnapshotting subscribers together under one short lock — with only the\ncheap `reserve_seq` inside it and the expensive `commit_slot` (slot\nwrite + wake) after release. **This PR just creates that seam**; no lock\nor caller yet. The reserved-but-uncommitted window is already\nreader-safe (readers see \"not ready\" and re-park until the commit wakes\nthem). `first` mode builds no required set, so it stays lock-free.\n\n## How the consensus resolves\n\nA message registered with required subscribers `{A, B, C}`:\n\n- stays **pending** as acks arrive, until the set empties -> **Ack**;\n- **Nack** the moment any required subscriber nacks or disappears\n(lag-disconnect/drop) before acking;\n- **Ack** immediately if there are zero eligible subscribers at publish\ntime;\n- ignores duplicate/late acks once resolved (first outcome wins; permit\nreleased exactly once).\n\nTopic close still resolves as `TopicClosed`, not Nack. The\n`first`/untracked/timeout paths are untouched.\n\n## Relationship to `fanout_processor`\n\nBroadcast `all` is the cross-pipeline cousin of `fanout`'s `await_ack:\nall` — same *\"all must Ack, any Nack -> fail-fast\"* contract, different\nsubstrate:\n\n| | `fanout_processor` (`await_ack: all`) | Topic broadcast `ack_mode:\nall` |\n| --- | --- | --- |\n| Scope | Within one pipeline | Across pipelines (topic hop) |\n| Who must ack | Fixed config ports | Snapshotted per-message from live\nsubscribers |\n| Failure handling | Rich fallback routing | No fallback;\nnack/disappearance -> Nack |\n\n`fanout`'s \"late ack after timeout is ignored\" is the same\nfirst-outcome-wins rule the tracker uses.\n\n## Not in this PR\n\n- **PR 2:** subscriber registry + publish/ack/disconnect wiring that\nhonors `all`.\n- **PR 3:** `broadcast.ack_mode` config field, validation, controller\nmapping, docs, example, changelog.\n\n## What issue does this PR close?\n\n* Part of #2252 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nNo\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-22T21:59:01Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/42083ad132c2dce8958a0c6f39ede8431311dcd6"
        },
        "date": 1782187459209,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.19,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.6,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Swapnil Ashtekar",
            "username": "swashtek",
            "email": "46826200+swashtek@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "2785c4fdd9b4bb83274b3095948901a21abd1aeb",
          "message": "etw_receiver: update attribute keys to use dot notation for consistency (#3346)\n\n# Change Summary\nupdate attribute keys to use dot notation for consistency\n\n## What issue does this PR close?\n* Closes #NNN\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\nNo\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-24T00:47:38Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2785c4fdd9b4bb83274b3095948901a21abd1aeb"
        },
        "date": 1782274048768,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7070b742b4f6e08137daf1efeee30987df3fa0aa",
          "message": "chore: bind test/dev servers to loopback to avoid Windows firewall prompts (#3353)\n\n## What\n\nOn Windows, running the Rust test suite — especially a full `cargo xtask\ncheck` —\nrepeatedly triggers the Windows Defender Firewall allow/deny prompt. A\nhandful of\n**test/dev** code paths bind listening sockets to `0.0.0.0` (all\ninterfaces);\nWindows exempts loopback (`127.0.0.1`) binds from the prompt. Because\ncargo names\ntest binaries by content hash, a granted \"Allow\" does not persist across\nrebuilds, so the prompt recurs on essentially every change.\n\nFixes #3351.\n\n## Changes\n\n- **`crates/telemetry/src/otel_sdk/meter_provider.rs`** (test): the\nPrometheus\n  pull-exporter test configured `host: \"0.0.0.0\", port: 9090`, which\n`MeterProvider::configure` → `start_async_prometheus_server` actually\nbinds.\nThis is the **only test that opens a non-loopback socket during `cargo\ntest`**.\n  Host → `127.0.0.1`.\n- **`crates/contrib-nodes/examples/mock_la_server.rs`** (dev example):\nmock Azure\nMonitor Logs Ingestion server, documented for `http://localhost:9999`,\nbound\n  `0.0.0.0:{port}`. Bind + banner → `127.0.0.1`.\n- **`CONTRIBUTING.md`**: documents the loopback test/example bind\nconvention so\n  this stays consistent.\n\n## Acceptance criterion\n\nA full `cargo xtask check` on Windows now completes with no Windows\nFirewall\nprompts — no test server binds a non-loopback interface. Verified\nlocally:\nstructure ✅, `cargo fmt` ✅, `cargo clippy --workspace --all-targets -D\nwarnings`\n✅, `cargo test --workspace` ✅. The example compiles under\n`--features azure-monitor-exporter`.\n\n## Scope / non-goals (deliberate)\n\n- **`GrpcServerSettings::default()` (`server_settings.rs:316`,\n`0.0.0.0:0`) is left\nunchanged.** It is a production default, and a full sweep confirms **no\ntest\nbinds via the bare default** — every gRPC/HTTP/UDP server test passes an\nexplicit `127.0.0.1` address. Per the issue's non-goals, production\ndefaults\nthat intentionally serve external traffic stay as-is (same for the\nPrometheus\n  pull `default_host()`).\n- **Other `0.0.0.0` occurrences in the Rust workspace are left as-is\nbecause they\ndo not bind a socket:** deserialize/`validate_config` tests that assert\nthe kept\nproduction default (`readers.rs`, `pull.rs`,\n`prometheus_exporter_provider.rs`),\nthe `otlp_receiver` test that specifically exercises the\nunspecified-address\n  conflict path, the clap arg-parse test, and doc/CIDR/CEF strings.\n- **Out of scope:** non-Rust demo/perf assets that intentionally bind\nall\ninterfaces (e.g. the Python `tools/pipeline_perf_test` Docker templates,\nand the\nbundled demo `configs/*.yaml`). These are not exercised by `cargo xtask\ncheck`\nand do not affect the acceptance criterion; a Windows dev who manually\nruns one\n  of those demos may still see a prompt.\n- The fixed port `9090` in the meter-provider test is kept (the issue\nasked only\nto change the host); the spawned bind result is ignored, so a port\ncollision\n  cannot fail the test.\n\n## Changelog\n\nTest/dev-example only — no shipped/production behavior change — so this\nis a\n`chore` with no `.chloggen` entry, per the repo convention.\n\n## On a regression guard\n\nAn automated lint that rejects new non-loopback binds in test/example\ncode was\nconsidered but **deferred**: it would need an allow-list for the several\nlegitimate non-binding `0.0.0.0` usages above, making it brittle. The\nconvention\nis documented in `CONTRIBUTING.md` instead.\n\nReply posted by [GS] @timr-dev Copilot AI Assistant.\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-06-24T22:51:01Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7070b742b4f6e08137daf1efeee30987df3fa0aa"
        },
        "date": 1782360436933,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "clhain",
            "username": "clhain",
            "email": "8164192+clhain@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "510247bf32178590394681e082fac547cc05cc50",
          "message": "Fix asymmetric Serialize/Deserialize on AttributeValue (#3359)\n\n# Change Summary\n\n`AttributeValue` and `AttributeValueArray` used a **derived\n`Serialize`** (serde's default externally-tagged enum format) but a\n**custom `Deserialize`** (expecting plain scalars and sequences). These\nwere incompatible: YAML/JSON produced by serialization could not be\ndeserialized back.\n\nThis broke any code path that round-tripped an `OtelDataflowSpec`\nthrough serialization — for example, CRD serialization for Kubernetes or\nconfig export/reimport.\n\n### Before (broken)\n\nSerializing a `TelemetryConfig` with resource attributes produced\nexternally-tagged enum output:\n\n**YAML:**\n```yaml\nresource:\n  k8s.container.name: !String ${env:K8S_CONTAINER_NAME}\n  k8s.pod.uid: !String ${env:K8S_POD_UID}\n  enabled: !Bool true\n  replica: !I64 3\n  tags: !Bool\n  - true\n  - false\n```\n\n**JSON:**\n```json\n{\n  \"k8s.container.name\": {\"String\": \"${env:K8S_CONTAINER_NAME}\"},\n  \"enabled\": {\"Bool\": true},\n  \"tags\": {\"Bool\": [true, false]}\n}\n```\n\nAttempting to deserialize this output back would fail with:\n```\ninvalid type: enum, expected a string, boolean, number, or array\n```\n\n### After (fixed)\n\nSerialization now emits plain scalars and bare sequences, matching what\nthe custom `Deserialize` expects:\n\n**YAML:**\n```yaml\nresource:\n  k8s.container.name: ${env:K8S_CONTAINER_NAME}\n  k8s.pod.uid: ${env:K8S_POD_UID}\n  enabled: true\n  replica: 3\n  tags:\n  - true\n  - false\n```\n\n**JSON:**\n```json\n{\n  \"k8s.container.name\": \"${env:K8S_CONTAINER_NAME}\",\n  \"enabled\": true,\n  \"tags\": [true, false]\n}\n```\n\nRound-tripping through `serde_yaml::to_string` / `serde_yaml::from_str`\n(and the JSON equivalents) now works correctly.\n\n## What issue does this PR close?\n\n* Closes #3358\n\n## How are these changes tested?\n\nFixed tests asserting the broken behavior and added:\n\n- **`test_attribute_value_yaml_roundtrip_scalars`** — Verifies all\nscalar variants (`String`, `Bool`, `I64`, `F64`) survive a YAML\nserialize/deserialize round-trip.\n- **`test_attribute_value_yaml_roundtrip_arrays`** — Verifies all array\nvariants (`String`, `Bool`, `I64`, `F64`) survive a YAML round-trip.\n- **`test_attribute_value_json_roundtrip`** — Verifies mixed scalars and\narrays survive a JSON round-trip.\n- **`test_attribute_value_serializes_plain_scalars_yaml`** — Asserts\nserialized YAML contains no enum tags (`!String`, `!I64`, etc.).\n- **`test_telemetry_config_yaml_roundtrip`** — Verifies a full\n`TelemetryConfig` with resource attributes round-trips through YAML.\n\n## Are there any user-facing changes?\n\nYes. Previously, serializing an OtelDataflowSpec (or any config\ncontaining AttributeValue fields like telemetry resource attributes) and\nthen deserializing it back would fail. This affected CRD round-trips,\nconfig export/reimport, and any serialize-then-deserialize workflow.\n\n### Changelog\n\n* [x] Added a .chloggen/*.yaml entry, OR this PR is a chore (indicated\nin title).\n\nCreated: .chloggen/fix-attribute-value-serialize-roundtrip.yaml",
          "timestamp": "2026-06-25T23:15:31Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/510247bf32178590394681e082fac547cc05cc50"
        },
        "date": 1782447274427,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.85,
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
          "id": "80a995ddbf4fd597c897dae3b5ebfcffbbd97dfd",
          "message": "chore(repo): Revise pull request template and AGENTS.md for better guidance (#3371)\n\nUpdated the pull request template to improve clarity and instructions.\n\n# Change Summary\n\nUpdate PR template to mention doc-only PRs to prevent AI comments like\nhttps://github.com/open-telemetry/otel-arrow/pull/3369#discussion_r3484154450:\n\n> PR description indicates the changelog requirement is satisfied\n(either a .chloggen/*.yaml entry was added or the PR title is a chore),\nbut this PR appears to add only documentation and the title is docs:.\nPer rust/otap-dataflow/AGENTS.md:110-123, skipping a Rust changelog\nentry requires chore in the PR title; otherwise add a .chloggen/*.yaml\nentry if this doc is considered user-facing.\n\n## What issue does this PR close?\n\nN/A\n\n## How are these changes tested?\n\nN/A\n\n## Are there any user-facing changes?\n\nYes, changes to PR description\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-06-26T22:00:44Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/80a995ddbf4fd597c897dae3b5ebfcffbbd97dfd"
        },
        "date": 1782532620391,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
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
          "id": "80a995ddbf4fd597c897dae3b5ebfcffbbd97dfd",
          "message": "chore(repo): Revise pull request template and AGENTS.md for better guidance (#3371)\n\nUpdated the pull request template to improve clarity and instructions.\n\n# Change Summary\n\nUpdate PR template to mention doc-only PRs to prevent AI comments like\nhttps://github.com/open-telemetry/otel-arrow/pull/3369#discussion_r3484154450:\n\n> PR description indicates the changelog requirement is satisfied\n(either a .chloggen/*.yaml entry was added or the PR title is a chore),\nbut this PR appears to add only documentation and the title is docs:.\nPer rust/otap-dataflow/AGENTS.md:110-123, skipping a Rust changelog\nentry requires chore in the PR title; otherwise add a .chloggen/*.yaml\nentry if this doc is considered user-facing.\n\n## What issue does this PR close?\n\nN/A\n\n## How are these changes tested?\n\nN/A\n\n## Are there any user-facing changes?\n\nYes, changes to PR description\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-06-26T22:00:44Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/80a995ddbf4fd597c897dae3b5ebfcffbbd97dfd"
        },
        "date": 1782620438588,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
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
          "id": "80a995ddbf4fd597c897dae3b5ebfcffbbd97dfd",
          "message": "chore(repo): Revise pull request template and AGENTS.md for better guidance (#3371)\n\nUpdated the pull request template to improve clarity and instructions.\n\n# Change Summary\n\nUpdate PR template to mention doc-only PRs to prevent AI comments like\nhttps://github.com/open-telemetry/otel-arrow/pull/3369#discussion_r3484154450:\n\n> PR description indicates the changelog requirement is satisfied\n(either a .chloggen/*.yaml entry was added or the PR title is a chore),\nbut this PR appears to add only documentation and the title is docs:.\nPer rust/otap-dataflow/AGENTS.md:110-123, skipping a Rust changelog\nentry requires chore in the PR title; otherwise add a .chloggen/*.yaml\nentry if this doc is considered user-facing.\n\n## What issue does this PR close?\n\nN/A\n\n## How are these changes tested?\n\nN/A\n\n## Are there any user-facing changes?\n\nYes, changes to PR description\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-06-26T22:00:44Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/80a995ddbf4fd597c897dae3b5ebfcffbbd97dfd"
        },
        "date": 1782707231894,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
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
          "id": "bdd514ccf95d7fbac61746c699c0ebc295eed900",
          "message": "chore(deps): update geneva-uploader digest to 48529c2 (#3376)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| geneva-uploader | workspace.dependencies | digest | `70f2dd3` →\n`48529c2` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on Monday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0My4yNDIuMiIsInVwZGF0ZWRJblZlciI6IjQzLjI0Mi4yIiwidGFyZ2V0QnJhbmNoIjoibWFpbiIsImxhYmVscyI6WyJkZXBlbmRlbmNpZXMiXX0=-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-06-29T15:41:04Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/bdd514ccf95d7fbac61746c699c0ebc295eed900"
        },
        "date": 1782792619491,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.72,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.03,
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
          "id": "9b6ee5b909a7a416bafbe185f4c3aee07ea283ad",
          "message": "feat(query-engine): support nested serialized attribute assignment (#3350)\n\n# Change Summary\n\n<!--\nReplace with a brief summary of the change in this PR\n-->\n\n## What issue does this PR close?\n\n<!--\nWe highly recommend correlation of every PR to an issue\n-->\n\n* Closes #3343\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry, OR this PR is a `chore`\n(indicated in title).",
          "timestamp": "2026-06-30T18:39:04Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9b6ee5b909a7a416bafbe185f4c3aee07ea283ad"
        },
        "date": 1782879885419,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.79,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.1,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "474998ab250661c7f54ab1819aa0029a06afbb3f",
          "message": "fix(otap): default gRPC server settings to loopback bind (#3400)\n\n## Summary\n\n`GrpcServerSettings::default()`\n(`crates/otap/src/otap_grpc/server_settings.rs`) defaulted\n`listening_addr` to\n`([0, 0, 0, 0], 0)` = **`0.0.0.0:0`** (all interfaces, ephemeral port).\nThe\nephemeral port `0` shows this default is test/builder-oriented (a real\nserver\nneeds a known port), but an all-interfaces bind triggers the **Windows\nDefender\nFirewall prompt** during `cargo test` — the exact issue the \"Test and\nexample\nserver bind addresses\" guidance in `CONTRIBUTING.md` (added in #3353)\nexists to\nprevent.\n\nThis changes the default to **`127.0.0.1:0`** (loopback ephemeral).\n\n## Why this is safe\n\n- `listening_addr` is a **required** config field (the struct is\n`#[serde(deny_unknown_fields)]` with no `#[serde(default)]` on the\nfield), so\nconfig-driven deployments always set it explicitly — production behavior\nis\n  unchanged.\n- The only in-tree consumer of the bare default today is a compression\nunit\ntest that never binds a socket; this is a latent-risk fix, not a\nbehavior fix\n  for existing tests.\n- A server that must serve external traffic still sets an explicit\n  all-interfaces address in config (`CONTRIBUTING.md` explicitly allows\n  production defaults to bind `0.0.0.0`).\n\n## Testing\n\n- New regression test `default_listening_addr_is_loopback` pins the\ndefault to a\nloopback address — it fails if the default is ever changed back to\n`0.0.0.0` /\n  `[::]`.\n- `cargo clippy -p otap-df-otap --all-targets -- -D warnings` — clean.\n- `cargo test -p otap-df-otap --lib server_settings` — passes.\n- `cargo fmt` — clean.\n\nChangelog: a `bug_fix` entry under `.chloggen` (component `otap`), since\n`GrpcServerSettings` is a `pub` type and its `Default` bind behavior\nchanges for\nprogrammatic Rust API consumers (the crate is `publish = false`, so\nthere are no\nexternal consumers).\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-07-01T22:40:33Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/474998ab250661c7f54ab1819aa0029a06afbb3f"
        },
        "date": 1782965154397,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.78,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "474998ab250661c7f54ab1819aa0029a06afbb3f",
          "message": "fix(otap): default gRPC server settings to loopback bind (#3400)\n\n## Summary\n\n`GrpcServerSettings::default()`\n(`crates/otap/src/otap_grpc/server_settings.rs`) defaulted\n`listening_addr` to\n`([0, 0, 0, 0], 0)` = **`0.0.0.0:0`** (all interfaces, ephemeral port).\nThe\nephemeral port `0` shows this default is test/builder-oriented (a real\nserver\nneeds a known port), but an all-interfaces bind triggers the **Windows\nDefender\nFirewall prompt** during `cargo test` — the exact issue the \"Test and\nexample\nserver bind addresses\" guidance in `CONTRIBUTING.md` (added in #3353)\nexists to\nprevent.\n\nThis changes the default to **`127.0.0.1:0`** (loopback ephemeral).\n\n## Why this is safe\n\n- `listening_addr` is a **required** config field (the struct is\n`#[serde(deny_unknown_fields)]` with no `#[serde(default)]` on the\nfield), so\nconfig-driven deployments always set it explicitly — production behavior\nis\n  unchanged.\n- The only in-tree consumer of the bare default today is a compression\nunit\ntest that never binds a socket; this is a latent-risk fix, not a\nbehavior fix\n  for existing tests.\n- A server that must serve external traffic still sets an explicit\n  all-interfaces address in config (`CONTRIBUTING.md` explicitly allows\n  production defaults to bind `0.0.0.0`).\n\n## Testing\n\n- New regression test `default_listening_addr_is_loopback` pins the\ndefault to a\nloopback address — it fails if the default is ever changed back to\n`0.0.0.0` /\n  `[::]`.\n- `cargo clippy -p otap-df-otap --all-targets -- -D warnings` — clean.\n- `cargo test -p otap-df-otap --lib server_settings` — passes.\n- `cargo fmt` — clean.\n\nChangelog: a `bug_fix` entry under `.chloggen` (component `otap`), since\n`GrpcServerSettings` is a `pub` type and its `Default` bind behavior\nchanges for\nprogrammatic Rust API consumers (the crate is `publish = false`, so\nthere are no\nexternal consumers).\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-07-01T22:40:33Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/474998ab250661c7f54ab1819aa0029a06afbb3f"
        },
        "date": 1783050064203,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.78,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "474998ab250661c7f54ab1819aa0029a06afbb3f",
          "message": "fix(otap): default gRPC server settings to loopback bind (#3400)\n\n## Summary\n\n`GrpcServerSettings::default()`\n(`crates/otap/src/otap_grpc/server_settings.rs`) defaulted\n`listening_addr` to\n`([0, 0, 0, 0], 0)` = **`0.0.0.0:0`** (all interfaces, ephemeral port).\nThe\nephemeral port `0` shows this default is test/builder-oriented (a real\nserver\nneeds a known port), but an all-interfaces bind triggers the **Windows\nDefender\nFirewall prompt** during `cargo test` — the exact issue the \"Test and\nexample\nserver bind addresses\" guidance in `CONTRIBUTING.md` (added in #3353)\nexists to\nprevent.\n\nThis changes the default to **`127.0.0.1:0`** (loopback ephemeral).\n\n## Why this is safe\n\n- `listening_addr` is a **required** config field (the struct is\n`#[serde(deny_unknown_fields)]` with no `#[serde(default)]` on the\nfield), so\nconfig-driven deployments always set it explicitly — production behavior\nis\n  unchanged.\n- The only in-tree consumer of the bare default today is a compression\nunit\ntest that never binds a socket; this is a latent-risk fix, not a\nbehavior fix\n  for existing tests.\n- A server that must serve external traffic still sets an explicit\n  all-interfaces address in config (`CONTRIBUTING.md` explicitly allows\n  production defaults to bind `0.0.0.0`).\n\n## Testing\n\n- New regression test `default_listening_addr_is_loopback` pins the\ndefault to a\nloopback address — it fails if the default is ever changed back to\n`0.0.0.0` /\n  `[::]`.\n- `cargo clippy -p otap-df-otap --all-targets -- -D warnings` — clean.\n- `cargo test -p otap-df-otap --lib server_settings` — passes.\n- `cargo fmt` — clean.\n\nChangelog: a `bug_fix` entry under `.chloggen` (component `otap`), since\n`GrpcServerSettings` is a `pub` type and its `Default` bind behavior\nchanges for\nprogrammatic Rust API consumers (the crate is `publish = false`, so\nthere are no\nexternal consumers).\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-07-01T22:40:33Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/474998ab250661c7f54ab1819aa0029a06afbb3f"
        },
        "date": 1783136361261,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.78,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "474998ab250661c7f54ab1819aa0029a06afbb3f",
          "message": "fix(otap): default gRPC server settings to loopback bind (#3400)\n\n## Summary\n\n`GrpcServerSettings::default()`\n(`crates/otap/src/otap_grpc/server_settings.rs`) defaulted\n`listening_addr` to\n`([0, 0, 0, 0], 0)` = **`0.0.0.0:0`** (all interfaces, ephemeral port).\nThe\nephemeral port `0` shows this default is test/builder-oriented (a real\nserver\nneeds a known port), but an all-interfaces bind triggers the **Windows\nDefender\nFirewall prompt** during `cargo test` — the exact issue the \"Test and\nexample\nserver bind addresses\" guidance in `CONTRIBUTING.md` (added in #3353)\nexists to\nprevent.\n\nThis changes the default to **`127.0.0.1:0`** (loopback ephemeral).\n\n## Why this is safe\n\n- `listening_addr` is a **required** config field (the struct is\n`#[serde(deny_unknown_fields)]` with no `#[serde(default)]` on the\nfield), so\nconfig-driven deployments always set it explicitly — production behavior\nis\n  unchanged.\n- The only in-tree consumer of the bare default today is a compression\nunit\ntest that never binds a socket; this is a latent-risk fix, not a\nbehavior fix\n  for existing tests.\n- A server that must serve external traffic still sets an explicit\n  all-interfaces address in config (`CONTRIBUTING.md` explicitly allows\n  production defaults to bind `0.0.0.0`).\n\n## Testing\n\n- New regression test `default_listening_addr_is_loopback` pins the\ndefault to a\nloopback address — it fails if the default is ever changed back to\n`0.0.0.0` /\n  `[::]`.\n- `cargo clippy -p otap-df-otap --all-targets -- -D warnings` — clean.\n- `cargo test -p otap-df-otap --lib server_settings` — passes.\n- `cargo fmt` — clean.\n\nChangelog: a `bug_fix` entry under `.chloggen` (component `otap`), since\n`GrpcServerSettings` is a `pub` type and its `Default` bind behavior\nchanges for\nprogrammatic Rust API consumers (the crate is `publish = false`, so\nthere are no\nexternal consumers).\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-07-01T22:40:33Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/474998ab250661c7f54ab1819aa0029a06afbb3f"
        },
        "date": 1783223743033,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.78,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Tim R",
            "username": "timr-dev",
            "email": "68666585+timr-dev@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "474998ab250661c7f54ab1819aa0029a06afbb3f",
          "message": "fix(otap): default gRPC server settings to loopback bind (#3400)\n\n## Summary\n\n`GrpcServerSettings::default()`\n(`crates/otap/src/otap_grpc/server_settings.rs`) defaulted\n`listening_addr` to\n`([0, 0, 0, 0], 0)` = **`0.0.0.0:0`** (all interfaces, ephemeral port).\nThe\nephemeral port `0` shows this default is test/builder-oriented (a real\nserver\nneeds a known port), but an all-interfaces bind triggers the **Windows\nDefender\nFirewall prompt** during `cargo test` — the exact issue the \"Test and\nexample\nserver bind addresses\" guidance in `CONTRIBUTING.md` (added in #3353)\nexists to\nprevent.\n\nThis changes the default to **`127.0.0.1:0`** (loopback ephemeral).\n\n## Why this is safe\n\n- `listening_addr` is a **required** config field (the struct is\n`#[serde(deny_unknown_fields)]` with no `#[serde(default)]` on the\nfield), so\nconfig-driven deployments always set it explicitly — production behavior\nis\n  unchanged.\n- The only in-tree consumer of the bare default today is a compression\nunit\ntest that never binds a socket; this is a latent-risk fix, not a\nbehavior fix\n  for existing tests.\n- A server that must serve external traffic still sets an explicit\n  all-interfaces address in config (`CONTRIBUTING.md` explicitly allows\n  production defaults to bind `0.0.0.0`).\n\n## Testing\n\n- New regression test `default_listening_addr_is_loopback` pins the\ndefault to a\nloopback address — it fails if the default is ever changed back to\n`0.0.0.0` /\n  `[::]`.\n- `cargo clippy -p otap-df-otap --all-targets -- -D warnings` — clean.\n- `cargo test -p otap-df-otap --lib server_settings` — passes.\n- `cargo fmt` — clean.\n\nChangelog: a `bug_fix` entry under `.chloggen` (component `otap`), since\n`GrpcServerSettings` is a `pub` type and its `Default` bind behavior\nchanges for\nprogrammatic Rust API consumers (the crate is `publish = false`, so\nthere are no\nexternal consumers).\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-07-01T22:40:33Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/474998ab250661c7f54ab1819aa0029a06afbb3f"
        },
        "date": 1783310569844,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.78,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.16,
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
          "id": "1a3f044400083b1ad84ac35fd1039781372d12bc",
          "message": "feat(engine): add BearerTokenProvider capability (#3372)\n\n# Change Summary\n\nIntroduce the engine-side capability surface for the Azure Identity Auth\nextension:\n\n- BearerTokenProvider trait (via #[capability]) with get_token() and\ntoken_stream(), plus the secret-redacting BearerToken data type.\n- CapabilityError + CapabilityErrorSource to attribute runtime\ncapability failures to the failing (extension, capability) pair.\n- Re-export the local/shared trait variants from\n{local,shared}::capability.\n\n## What issue does this PR close?\n\n* Related to #3356\n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nNo\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-06T18:35:42Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1a3f044400083b1ad84ac35fd1039781372d12bc"
        },
        "date": 1783396472694,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.8,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.16,
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
          "id": "3d4c518f866d0e7dcebcc43ed1fe04b9f3941602",
          "message": "docs: opamp design doc (#3388)\n\n# Change Summary\n\n<!--Replace with a brief summary of the change in this PR-->\n\nAdds a design document discussing initial implementation of OpAMP Agent\ncontroller extension which can be used to receive configuration from a\nremote OpAMP server and provide status status to the remote server.\n\nThe purpose of the document is mostly to align on the overall initial\nimplementation and specifically align on structure of the configuration\n& messages plus how health/status information is derived.\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Relates to #3387\n\n## How are these changes tested?\n\nn/a\n\n## Are there any user-facing changes?\n\nno\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [x] This is a documentation-only PR.",
          "timestamp": "2026-07-07T23:52:57Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/3d4c518f866d0e7dcebcc43ed1fe04b9f3941602"
        },
        "date": 1783480234963,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.09,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.28,
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
          "id": "66541e92c02992528ccb1693cf5ea2269a13b230",
          "message": "chore(ci): restore continuous/nightly pipeline perf workflows (#3364)\n\n## Root cause\n\nAll four pipeline perf workflows set `TMPDIR`/`TMP`/`TEMP` in job-level\n`env:` using `${{ runner.temp }}`. The `runner` context is not available\nat job-level `env` scope (only inside `steps`), so GitHub Actions\nrejects the workflow at evaluation time:\n\n> Unrecognized named-value: 'runner'. Located at position 1 within\nexpression: runner.temp\n\nFailing since #3164 introduced the pattern on June 8.\n\n## Fix\n\nReplace the job-level `env` block with a `Route temp files to\nRUNNER_TEMP` step at the top of `steps:` that writes\n`TMPDIR`/`TMP`/`TEMP` to `$GITHUB_ENV` using `$RUNNER_TEMP`. Placed\nbefore `harden-runner` so every subsequent step inherits the routed temp\ndirs.",
          "timestamp": "2026-07-08T11:04:53Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/66541e92c02992528ccb1693cf5ea2269a13b230"
        },
        "date": 1783569322060,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.09,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.28,
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
          "id": "d2b3ed04a389abf5f9e460123d95ea42b30312bf",
          "message": "feat: add opamp controller extension (#3410)\n\n# Change Summary\n\nAdds initial implementation of OpAMP Agent Controller Extension.\n\nDesign doc can be found in #3388 \n\nAdds:\n- controller extension\n- OpAMP proto definitions & prost generated structs\n\nCurrently only works with websocket.\n\nFollowups will need to include:\n- [plain HTTP\ntransport](https://opentelemetry.io/docs/specs/opamp/#plain-http-transport)\n(plain meaning traditional HTTP request/response, not necessarily\nplaintext)\n- mTLS\n- connection setting management (this is ignored)\n- metrics collection\n\nCurrently this lives in the controller crate. I wasn't sure where was\nthe right place for this, and hesitated between controller or\ncore-nodes. Happy to move if anyone has feelings/suggestions\n\n<!--Replace with a brief summary of the change in this PR-->\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Relates to #3387 \n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: cijothomas <cijo.thomas@gmail.com>\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>",
          "timestamp": "2026-07-09T17:01:29Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d2b3ed04a389abf5f9e460123d95ea42b30312bf"
        },
        "date": 1783655428674,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.38,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.78,
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
          "id": "59957e07eec5630be835293d6226289bcbfc0596",
          "message": "fix(pdata): Fix silent attribute loss when decoding OTAP batches whose root IDs are not monotonic in resource/scope visitation order (#3450)\n\n# Change Summary\n\nAddresses a follow-up from #2421.\n\nOn that PR review, it was noted that a 'truly pathological' OTAP batch\nmight drop attributes in certain cases if root IDs are not in expected\norder. However, with query-engine, it is easily possible to construct\nsuch a batch when adding attributes to a record which previously didn't\nhave any.\n\nThe OTLP decoder joined child records (attributes, datapoints, span\nevents/links) to parents with a shared forward-only cursor that assumed\nparents were visited in ascending ID order. When root IDs aren't\nmonotonic in `(resource_id, scope_id, id)` visitation order, a\nlater-visited smaller-ID record's child rows were skipped — silently\ndropping all of that record's attributes with no error.\n\n`ChildIndexIter::new` now binary-searches to each parent's rows\n(`SortedBatchCursor::seek_to_parent`), making the join order-independent\nacross logs, metrics, and traces. Adds regression tests in `pdata` and\n`query-engine`.\n\nThis could have a minor performance impact adding a `O(P * log n)`\nsearch where `P` is the number of parent records and `n` is the number\nof child rows.\n\n## What issue does this PR close?\n\n* Closes #3448 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-10T20:58:39Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/59957e07eec5630be835293d6226289bcbfc0596"
        },
        "date": 1783739121367,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.39,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.72,
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
          "id": "59957e07eec5630be835293d6226289bcbfc0596",
          "message": "fix(pdata): Fix silent attribute loss when decoding OTAP batches whose root IDs are not monotonic in resource/scope visitation order (#3450)\n\n# Change Summary\n\nAddresses a follow-up from #2421.\n\nOn that PR review, it was noted that a 'truly pathological' OTAP batch\nmight drop attributes in certain cases if root IDs are not in expected\norder. However, with query-engine, it is easily possible to construct\nsuch a batch when adding attributes to a record which previously didn't\nhave any.\n\nThe OTLP decoder joined child records (attributes, datapoints, span\nevents/links) to parents with a shared forward-only cursor that assumed\nparents were visited in ascending ID order. When root IDs aren't\nmonotonic in `(resource_id, scope_id, id)` visitation order, a\nlater-visited smaller-ID record's child rows were skipped — silently\ndropping all of that record's attributes with no error.\n\n`ChildIndexIter::new` now binary-searches to each parent's rows\n(`SortedBatchCursor::seek_to_parent`), making the join order-independent\nacross logs, metrics, and traces. Adds regression tests in `pdata` and\n`query-engine`.\n\nThis could have a minor performance impact adding a `O(P * log n)`\nsearch where `P` is the number of parent records and `n` is the number\nof child rows.\n\n## What issue does this PR close?\n\n* Closes #3448 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-10T20:58:39Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/59957e07eec5630be835293d6226289bcbfc0596"
        },
        "date": 1783826070587,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.39,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.72,
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
          "id": "59957e07eec5630be835293d6226289bcbfc0596",
          "message": "fix(pdata): Fix silent attribute loss when decoding OTAP batches whose root IDs are not monotonic in resource/scope visitation order (#3450)\n\n# Change Summary\n\nAddresses a follow-up from #2421.\n\nOn that PR review, it was noted that a 'truly pathological' OTAP batch\nmight drop attributes in certain cases if root IDs are not in expected\norder. However, with query-engine, it is easily possible to construct\nsuch a batch when adding attributes to a record which previously didn't\nhave any.\n\nThe OTLP decoder joined child records (attributes, datapoints, span\nevents/links) to parents with a shared forward-only cursor that assumed\nparents were visited in ascending ID order. When root IDs aren't\nmonotonic in `(resource_id, scope_id, id)` visitation order, a\nlater-visited smaller-ID record's child rows were skipped — silently\ndropping all of that record's attributes with no error.\n\n`ChildIndexIter::new` now binary-searches to each parent's rows\n(`SortedBatchCursor::seek_to_parent`), making the join order-independent\nacross logs, metrics, and traces. Adds regression tests in `pdata` and\n`query-engine`.\n\nThis could have a minor performance impact adding a `O(P * log n)`\nsearch where `P` is the number of parent records and `n` is the number\nof child rows.\n\n## What issue does this PR close?\n\n* Closes #3448 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-10T20:58:39Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/59957e07eec5630be835293d6226289bcbfc0596"
        },
        "date": 1783912530251,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.39,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.72,
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
          "id": "035e910605e727284dd65a849142868778665c89",
          "message": "chore: Optimize per-event field extraction in the ETW receiver (#3427)\n\n## Summary\n\nCaches per-schema field readers instead of rebuilding them every event\nand drops a redundant per-field copy.\n\n## Problem\nPer event, `extract_decoded_fields` called\n`EventFormat::try_get_field_data_closure(name)` once for every field.\nEach call does two costly things:\n- Finds the field by name by re-walking the field list from index 0.\nReading all n fields is therefore `1 + 2 + ... + n = O(n^2)` name\ncomparisons per event.\n- Heap-allocates a boxed closure (`Box<dyn FnMut>`) for the result, so\n`n` allocations per event.\n\nThe result was then `.to_vec()`'d before interpretation, adding another\n`n` copies. So, a single event with `n` fields cost `O(n^2)`\nname-finding + `~2n` allocations, repeated on every event on the decode\nthread.\n\n### Performance\n\nCaching field readers per schema (instead of rebuilding a boxed closure\nper field on every event) turns per-event field extraction from an\nO(n^2) name-walk + n allocations into an O(n) reuse. A microbenchmark\nreading 16 fixed-size fields:\n\n| | time (16 fields) |\n|---|---|\n| `closure_per_field` (before) | ~990 ns |\n| `cached_refs` (after) | ~72 ns |\n\n~14x faster. The gap compounds two effects the change removes: the\nO(n^2) re-walk to find each field by name, and a heap allocation (boxed\nclosure) per field per event.\n\n<details>\n<summary>Benchmark used to measure this (criterion, against\none_collect's public API)</summary>\n\n```rust\nuse criterion::{black_box, criterion_group, criterion_main, Criterion};\nuse one_collect::event::{EventField, EventFormat, LocationType};\n\nconst FIELD_COUNT: usize = 16;\n\nfn bench_field_reads(c: &mut Criterion) {\n    // All-fixed-size schema (the common TraceLogging shape after struct\n    // flattening), so absolute offsets are valid for the cached-reader path.\n    let mut format = EventFormat::new();\n    let mut names = Vec::with_capacity(FIELD_COUNT);\n    for i in 0..FIELD_COUNT {\n        let name = format!(\"f{i}\");\n        format.add_field(EventField::new(\n            name.clone(),\n            \"u32\".to_string(),\n            LocationType::Static,\n            i * 4,\n            4,\n        ));\n        names.push(name);\n    }\n\n    let data = vec![0xABu8; FIELD_COUNT * 4];\n    let data = data.as_slice();\n\n    // Readers resolved once (a consumer caches these per schema_id).\n    let refs: Vec<_> = names\n        .iter()\n        .map(|n| format.get_field_ref(n).expect(\"field exists\"))\n        .collect();\n\n    let mut group = c.benchmark_group(\"tdh_field_reads\");\n\n    // Before: a fresh boxed closure per field, per event (re-walks the field\n    // list each call, so reading all n fields is O(n^2) plus n allocations).\n    group.bench_function(\"closure_per_field\", |b| {\n        b.iter(|| {\n            for n in &names {\n                let mut reader = format\n                    .try_get_field_data_closure(n)\n                    .expect(\"field exists\");\n                black_box(reader(data));\n            }\n        });\n    });\n\n    // After: O(1), allocation-free reads via readers resolved once per schema.\n    group.bench_function(\"cached_refs\", |b| {\n        b.iter(|| {\n            for r in &refs {\n                black_box(format.get_data(*r, data));\n            }\n        });\n    });\n\n    group.finish();\n}\n\ncriterion_group!(benches, bench_field_reads);\ncriterion_main!(benches);\n```\n</details>\n\n## Changes\n- Cache field readers per `SchemaId`. The name-finding walk and the\nclosure boxing now happen once per schema (on first sight), and every\nlater event of that schema reuses the cached closures. Per-event\nextraction becomes an `O(n)` pass with no per-field closure allocations.\n- Drop the `to_vec`. The cached closure's borrowed slice is passed\nstraight to `interpret_field_value`, so numeric fields allocate nothing.\n- Bound the cache (MAX_CACHED_SCHEMAS = 4096) against pathological\nproducers that emit unbounded distinct schemas; beyond the cap, new\nschemas fall back to the original per-event path so the map can't grow\nwithout bound.\n- Pre-size to 128 to avoid warmup rehashes.\n\n## How are these changes tested?\n- Existing unit tests and integration tests\n\n## Are there any user-facing changes?\n\nNo\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: Swapnil Ashtekar <46826200+swashtek@users.noreply.github.com>",
          "timestamp": "2026-07-13T19:25:19Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/035e910605e727284dd65a849142868778665c89"
        },
        "date": 1783997878454,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.38,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.72,
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
          "id": "339e7ac67cc04ab693a08cb4692203f35bd99a0f",
          "message": "feat(recordset_kql_processor): Record signals.dropped in recordset_kql_processor (#3482)\n\n# Change Summary\n\nExtends the `signals.dropped` flow metric (#2859) to the recordset_kql\nprocessor so pipelines can observe how many records it filters out.\nTogether with the earlier transform-processor change, this gives every\nKQL-based decision node the same queryable dropped count already\nprovided by `filter_processor` and `log_sampling_processor`.\n\nVery similar to recent PR #3473.\n\n### Validation\n\nRan `configs/trafficgen-flow-metrics-demo.yaml` (with `--features\nrecordset-kql-processor`), which places the recordset_kql processor as\none of four interior decision nodes in a single `ingest_pipeline` flow\nrange: the sampler keeps ~2/3, filter drops `worker-1`, transform drops\n`worker-3`, and recordset drops `worker-2`. Each decision node's drops\nare tagged with a distinct `flow.node.decision`, and the counts\nreconcile exactly against incoming/outgoing (480 − 160 − 48 − 48 − 48 =\n176).\n\n| `flow.node.decision` | Metric | Sum | Count |\n| --- | --- | ---: | ---: |\n| _(range)_ | signals.incoming | 480 | 48 |\n| sampler | signals.dropped | 160 | 48 |\n| filter | signals.dropped | 48 | 48 |\n| transform | signals.dropped | 48 | 48 |\n| **recordset** | **signals.dropped** | **48** | 48 |\n| _(range)_ | signals.outgoing | 176 | 48 |\n\nThe `recordset` row confirms the processor records `signals.dropped`\nunder its own decision attribute, exactly like the existing\n`filter`/`transform`/`sampler` decision nodes.\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Related to #2859 \n\n## How are these changes tested?\n\n* Unit tests and demo config\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\nAdditional `flow.dropped` metric source\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-14T23:32:04Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/339e7ac67cc04ab693a08cb4692203f35bd99a0f"
        },
        "date": 1784085631652,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.72,
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
          "id": "aa051fe5dc924e81dcc0ef075de4833a0f259b6d",
          "message": "feat(metrics): Add datapoint-level enum-attribute mechanism for metric sets (#3454)\n\n# Change Summary\n\nAdds the core plumbing for datapoint-level enum attributes on metric\nsets\n\n### Outcome\n\nInstrumentation can now declare closed-set (`enum`) attributes on the\n**existing** `metric_set` unit — no new instrument family, no per-signal\nset explosion — in two flavors:\n\n- **Registration** attributes — a value fixed once at registration,\nattached to every datapoint of the set (e.g. `signal = logs` on a\njournald receiver).\n- **Measurement** attributes — values that vary per recorded datapoint\n(e.g. `signal` × `outcome` for durable-buffer loss). Each combination is\nrecorded through a generated `with(attrs)` and exported as its own\ndatapoint with the right attributes.\n\nWorst-case cardinality of a metric set is known at compile time, and a\nset that exceeds the budget (2000) is **rejected with a hard build\nerror** at the declaration site — so cardinality blowups are caught at\ncompile time rather than in production.\n\nPlain metric sets are unaffected. **No node instrumentation is migrated\nin this PR** — that is a separate sub-issue.\n\n### Usage\n\n```rust\n#[derive(Debug, Clone, Copy, AttributeEnum)]\npub enum Signal {\n    #[attribute_value = \"log-records\"] // optional rename\n    Logs,\n    Metrics,\n    Traces,\n}\n\n#[derive(Debug, Clone, Copy, AttributeEnum)]\npub enum LossOutcome {\n    Dropped,\n    Expired,\n}\n\n#[attribute_set(name = \"durable_buffer.loss.attrs\", measurement)]\n#[derive(Debug, Clone, Copy)]\npub struct LossAttributes {\n    pub signal: Signal,\n    #[attribute_key = \"loss.outcome\"] // optional rename\n    pub outcome: LossOutcome,\n}\n\n#[metric_set(name = \"processor.durable_buffer.loss\", measurement_attributes = LossAttributes)]\n#[derive(Debug, Default, Clone)]\npub struct LossMetrics {\n    #[metric(unit = \"{items}\")]\n    pub lost_items: Counter<u64>,\n}\n\nlet mut loss = LossMetrics::register(&pipeline_ctx);\nloss.with(LossAttributes {\n    signal: Signal::Metrics,\n    outcome: LossOutcome::Expired,\n})\n.lost_items\n.add(80); // signal=metrics, loss.outcome=expired\n\n#[attribute_set(name = \"signal.attrs\")]\n#[derive(Debug, Clone, Copy)]\npub struct SignalAttributes {\n    pub signal: Signal,\n}\n\n#[metric_set(name = \"receiver.journald\", registration_attributes = SignalAttributes)]\n#[derive(Debug, Default, Clone)]\npub struct JournaldMetrics {\n    #[metric(unit = \"{records}\")]\n    pub records: Counter<u64>,\n}\n\nlet mut metrics = JournaldMetrics::register(\n    &pipeline_ctx,\n    &SignalAttributes {\n        signal: Signal::Logs,\n    },\n);\nmetrics.records.add(42); // signal=log-records\n```\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Part of #3300\n* Closes #3430\n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nNo\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-15T23:07:25Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/aa051fe5dc924e81dcc0ef075de4833a0f259b6d"
        },
        "date": 1784171002420,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.72,
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
          "id": "a1904eee5d1e84b07820ba4a93e0a2b22c05282f",
          "message": "  feat(pdata): add retained memory sizing (#3443)\n\n# Change Summary\n\nAdds a pdata-level retained memory size API without changing existing\nencoded-size semantics.\n\nThe new API gives retention sites a way to estimate how much memory a\npayload keeps alive:\n  - `OtapArrowRecords::retained_memory_bytes()`\n  - `OtapPayload::retained_memory_bytes()`\n  - `OtapPayloadHelpers::retained_memory_bytes()`\n\nFor OTAP Arrow records, this walks Arrow buffers and dedupes shared\nbuffers within one pdata accounting call. `num_bytes()` is unchanged and\nstill represents encoded/wire size.\n\n## What issue does this PR close?\n\n* Closes #3442\n\n## How are these changes tested?\n\n  - `cargo fmt --all`\n  - `cargo check -p otap-df-pdata`\n  - `cargo clippy -p otap-df-pdata --all-targets -- -D warnings`\n  - `cargo test -p otap-df-pdata`\n  - `python3 tools/sanitycheck.py`\n\n## Are there any user-facing changes?\n\n  Yes. This adds a public pdata helper API.\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-16T19:22:55Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a1904eee5d1e84b07820ba4a93e0a2b22c05282f"
        },
        "date": 1784257527396,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 112.43,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.78,
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
          "id": "4ab1d242e454fc5eaacb68118e06ca37151b576a",
          "message": "[otap-dataflow] add kafka exporter into contrib-nodes (#3262)\n\n# Change Summary\n\nAdd Kafka Exporter implementation that takes inspiration from the go and\nrotel versions.\n\nAdd kafka_util that shares common functions and data types with the\nkafka receiver and exporter\n\n## What issue does this PR close?\n\n* Closes #3249 \n\n## How are these changes tested?\n\nunit tests and integration tests with kafka broker (requires docker\ncontainer)\n\n## Are there any user-facing changes?\n\nno user face changes",
          "timestamp": "2026-07-17T23:40:28Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/4ab1d242e454fc5eaacb68118e06ca37151b576a"
        },
        "date": 1784343423781,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 113.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
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
          "id": "de748c94f1a775b450ca6066b62fb5b2c8f281cd",
          "message": "[otap-dataflow] add kafka receiver into contrib-nodes (#3261)\n\n# Change Summary\n\nAdd Kafka Receiver implementation that takes inspiration from the go and\nrotel versions.\n\nAdd kafka_util that shares common functions and data types with the\nkafka receiver and exporter\n\n## What issue does this PR close?\n\n* Closes #3248 \n\n## How are these changes tested?\n\nunit tests and integration tests with kafka broker (requires docker\ncontainer)\n\n## Are there any user-facing changes?\n\nno user face changes\n\n---------\n\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-07-18T21:03:59Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/de748c94f1a775b450ca6066b62fb5b2c8f281cd"
        },
        "date": 1784430688240,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 113.33,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
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
          "id": "de748c94f1a775b450ca6066b62fb5b2c8f281cd",
          "message": "[otap-dataflow] add kafka receiver into contrib-nodes (#3261)\n\n# Change Summary\n\nAdd Kafka Receiver implementation that takes inspiration from the go and\nrotel versions.\n\nAdd kafka_util that shares common functions and data types with the\nkafka receiver and exporter\n\n## What issue does this PR close?\n\n* Closes #3248 \n\n## How are these changes tested?\n\nunit tests and integration tests with kafka broker (requires docker\ncontainer)\n\n## Are there any user-facing changes?\n\nno user face changes\n\n---------\n\nCo-authored-by: Laurent Quérel <laurent.querel@gmail.com>\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-07-18T21:03:59Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/de748c94f1a775b450ca6066b62fb5b2c8f281cd"
        },
        "date": 1784518842589,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 113.33,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
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
          "id": "7502e7dbe636b6bd14d15e0a69367fa00bc10343",
          "message": "feat(metrics): Require scope keyword on scope attribute_set declarations (#3531)\n\n# Change Summary\n\nRequire `scope` on every scope-level `#[attribute_set]` declaration so\nthe intended telemetry attachment point is explicit and unambiguous.\n\nFollow-up from\nhttps://github.com/open-telemetry/otel-arrow/pull/3499#issuecomment-5004841970\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Closes #3513\n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-20T22:13:43Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7502e7dbe636b6bd14d15e0a69367fa00bc10343"
        },
        "date": 1784603906360,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 113.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.66,
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
          "id": "d25a5680f19dfa4f53f2e5c3179ce07ea2c8d3f8",
          "message": "chore: Fix flaky quiver WAL replay tests by disabling time-based segment finalization (#3533)\n\n# Change Summary\n\n## Problem\n`wal_replay_reads_from_rotated_files` intermittently fails in CI with:\n\n```\nassertion `left == right` failed: no segments should be finalized\n  left: 2, right: 0\n```\n\nBoth this test and `wal_replay_finalizes_segments_if_threshold_exceeded`\nset a large `target_size_bytes` and assert that ingesting 20 bundles\nfinalizes zero segments (so the data stays in the WAL). But segment\nfinalization also triggers on `max_open_duration`, which defaulted to 5\nseconds. On a loaded CI runner, ingesting the bundles occasionally\nexceeded 5 seconds of wall-clock time, causing time-based finalization\nand breaking the assertion.\n\nHere's a sample CI run that fails with these tests:\nhttps://github.com/open-telemetry/otel-arrow/actions/runs/29763886758/job/88433587864?pr=3528\n\n## Fix\n\nSet max_open_duration: `Duration::from_secs(3600)` in both tests'\n`SegmentConfig` so only size/stream limits (which the tests never hit)\ncan trigger finalization. This removes the wall-clock dependency and\nmakes the tests deterministic. It matches the convention already used\nelsewhere in this file.\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Closes #NNN\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\nTest-only change; no production behavior is affected.\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-21T21:54:35Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/d25a5680f19dfa4f53f2e5c3179ce07ea2c8d3f8"
        },
        "date": 1784693446717,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 113.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.66,
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
          "id": "7987c8b5f859c8febe4d0a1fc3e1b28dc48bc71e",
          "message": "feat(wasm-host): introduce experimental WASM host-kernel processor plugin (#3478)\n\n# Change Summary\n\n- Added `otap-df-wasm-host` crate for WASM host-kernel runtime.\n- Implemented simple `severity-filter` reference guest plugin to filter\nlog records by severity.\n- Created integration tests to validate the functionality of the WASM\nprocessor.\n- Established WIT contract for OTAP dataflow WASM plugins.\n- Introduced bridge and host modules for managing data between the host\nand guest.\n- Until stabilized, the binary plugins feature is disabled by default in\nbuilds and must be enabled with the `wasm` flag\n\n## What issue does this PR close?\n\n- Starts implementation of #2973 and #3227 \n\n## How are these changes tested?\n\n- Integration and unit tests included\n\n## Are there any user-facing changes?\n\n- When the `wasm` flag is enabled, builds the experimental\n`otap-df-wasm-host` crate and support for WASM binary plugins.\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>",
          "timestamp": "2026-07-22T21:56:11Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7987c8b5f859c8febe4d0a1fc3e1b28dc48bc71e"
        },
        "date": 1784776447802,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.78,
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
          "id": "9d6e1ce4a5b728dac98d1e60a36ba5285e50dc82",
          "message": "refactor(geneva): use geneva-uploader tls-rustls feature to enable the SymCrypt path (#3408)\n\n## Change Summary\n\nEnables geneva-uploader's `tls-rustls` feature (default features off),\nswitching the\nGeneva exporter from native-tls to rustls:\n\n```toml\ngeneva-uploader = { git = \"...\", rev = \"70f2dd38...\", default-features = false, features = [\"tls-rustls\"] }\n```\n\nThe Geneva exporter was the only TLS component still on native-tls\n(OpenSSL), which\nbypassed the pluggable rustls crypto provider. It now rides the\nprocess-wide provider\ninstalled at startup, so Geneva uploads work end-to-end with SymCrypt\n(`crypto-symcrypt`),\nconsistent with the rest of otap-dataflow.\n\n## Changes\n\n- **`Cargo.toml`**: enable `tls-rustls`.\n- **`Cargo.lock`**: drops `native-tls`/`hyper-tls`/`tokio-native-tls`\nfrom the Geneva path\nand adds the rustls stack + `p12-keystore` and its closure (`cbc`,\n`des`, `rc2`, `scrypt`,\n`pkcs12`, `x509-cert`, …). These parse the PKCS#12 client cert for\nGeneva mTLS — previously\n  handled by the OS cert store via native-tls, and unique to Geneva.\n- **`geneva_exporter/mod.rs` (tests)**: added the idempotent\n`otap_df_otap::crypto::ensure_crypto_provider()` to the 3 tests that\nbuild a `GenevaClient`\n(reqwest/rustls now needs a provider; production installs it at\nstartup).\n- **`rust-ci.yml`**: added `geneva-exporter` to the Windows\n`crypto-symcrypt` build so the\n  Geneva + SymCrypt path is compiled/linked in CI.\n\n## Note\n\nBuilds enabling `geneva-exporter` must also enable one `crypto-*`\nfeature (`crypto-ring` by\ndefault), else TLS fails at runtime — the same contract documented in\n`crypto.rs`.\n\n## Validation\n\n`cargo test -p otap-df-contrib-nodes --features\n\"geneva-exporter,otap-df-otap/crypto-ring\"`,\nclippy `--all-targets -- -D warnings`, and `cargo fmt --check` all pass.\nNo default behavior\nchange (`crypto-ring` stays default); SymCrypt routing is opt-in via\n`crypto-symcrypt`.\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>\nCopilot-Session: 1e52f75b-8536-477a-8685-1236e3d714e3\nCopilot-Session: f07b7fb6-592f-442c-8227-5a77c7895d58",
          "timestamp": "2026-07-23T22:06:38Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/9d6e1ce4a5b728dac98d1e60a36ba5285e50dc82"
        },
        "date": 1784862547691,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.78,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Swapnil Ashtekar",
            "username": "swashtek",
            "email": "46826200+swashtek@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "eaf8f4cca694c396409f772a902b1ef512813f3e",
          "message": "chore: improve GUID formatting and clarify comments in encoder and tests (#3537)\n\n# Change Summary\nchore: improve GUID formatting and clarify comments in encoder and tests\n\n## What issue does this PR close?\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\nNo\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-25T00:36:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/eaf8f4cca694c396409f772a902b1ef512813f3e"
        },
        "date": 1784952310718,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Swapnil Ashtekar",
            "username": "swashtek",
            "email": "46826200+swashtek@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "eaf8f4cca694c396409f772a902b1ef512813f3e",
          "message": "chore: improve GUID formatting and clarify comments in encoder and tests (#3537)\n\n# Change Summary\nchore: improve GUID formatting and clarify comments in encoder and tests\n\n## What issue does this PR close?\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\nNo\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-25T00:36:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/eaf8f4cca694c396409f772a902b1ef512813f3e"
        },
        "date": 1785035782656,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Swapnil Ashtekar",
            "username": "swashtek",
            "email": "46826200+swashtek@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "eaf8f4cca694c396409f772a902b1ef512813f3e",
          "message": "chore: improve GUID formatting and clarify comments in encoder and tests (#3537)\n\n# Change Summary\nchore: improve GUID formatting and clarify comments in encoder and tests\n\n## What issue does this PR close?\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\nNo\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-25T00:36:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/eaf8f4cca694c396409f772a902b1ef512813f3e"
        },
        "date": 1785123737164,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.42,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "David Dahl",
            "username": "daviddahl",
            "email": "d.dahl@f5.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "537d4ecc1d18287999b4e8573ac2aa29ec447e5c",
          "message": "feat(engine): #[component_inventory] macro + inventory module (RFC 0001 Phase 1) (#3487)\n\n# Change Summary\n\nImplements **Phase 1 of RFC 0001 (component inventory)** — the\n`#[component_inventory]` attribute macro and its runtime metadata\nsurface,\nmirroring the existing `#[capability]` → `KNOWN_CAPABILITIES` mechanism.\n\nThis is the first of the four staged PRs from the RFC / tracking issue\n#3435.\nIt adds **no component annotations** (Phase 2), **no `xtask` command**\n(Phase 3), and **no CI enforcement** (Phase 4). Zero runtime cost; no\nnew\ncrates.\n\n## What's in this PR\n\n**`otap-df-engine` — new `inventory` module\n(`crates/engine/src/inventory.rs`):**\n\n- `ComponentMeta { id, category, description, file, line, attributes }`\nand a\n`#[linkme::distributed_slice] COMPONENT_INVENTORY` populated at link\ntime,\nplus a `components()` accessor and `ComponentMeta::attribute(key)`\nhelper.\n- `Category` enum — **Phase 1 ships only the four factory categories**\n  (`Receiver`, `Exporter`, `Processor`, `Extension`), each with\n  `urn_segment()` for the macro's URN cross-check.\n- `attrs` key constants (RFC **Option A**): the attribute map stays\nfree-form\n  `&[(&str, &str)]`, with `PORT`/`PROTOCOL`/`AUTH`/… key constants for\nconsistency. Value validation (Option C) is intentionally **not** in\nPhase 1.\n\n**`otap-df-engine-macros` — new `#[component_inventory]` proc macro:**\n\n- Re-emits the annotated item unchanged and appends one\n`COMPONENT_INVENTORY`\nentry using fully-qualified `::otap_df_engine::inventory::*` paths, so\nit can\nbe invoked from any node crate (unlike `#[capability]`, which is\nengine-local).\n- **Factory case:** `id` is derived from the factory static's `name`\n(URN)\nfield — contributors write no `id`. **Non-factory items require an\nexplicit\n  URN-shaped `id`.**\n- `category` is a validated bare identifier (a misspelling like\n`Reciever` is a\ncompile error). When the URN is a string literal, `category` is\ncross-checked\nagainst the URN segment; for `const`-path URNs the value isn't visible\nat\n  macro time, so the full cross-check is deferred to the Phase 3 `xtask`\n  scanner.\n- Propagates the annotated item's `#[cfg(...)]` onto the emitted entry,\nso the\n  inventory reflects exactly what was compiled.\n\n**Tests:**\n\n- Hand-rolled macro-expansion unit tests (arg parsing, `id` derivation,\n`cfg`\n  propagation, literal-URN cross-check, attribute ordering).\n- The compile-fail paths (unknown/missing `category`, missing `id` on a\nnon-factory item, URN/category mismatch) are covered by the same unit\ntests,\n  which assert on the generated error text. `trybuild` UI tests are\nintentionally **not** used: this repo runs tests via `cargo nextest`\nfrom a\nprebuilt archive in `--offline` mode, and trybuild spawns a nested\n`cargo`\n  build for its fixture crate that cannot resolve dependencies offline.\n- End-to-end test (`crates/engine/tests/component_inventory_e2e.rs`)\nthat\nannotates a factory-style static and a non-factory struct, then reads\nback\n  `COMPONENT_INVENTORY` — validating the cross-crate link-time path.\n\n## Deferred (called out explicitly for reviewers)\n\n- **Non-factory `Category` variants** (`Admin`, `Controller`, `Cli`,\n`Subsystem`, `Safety`) from the RFC are **deferred to Phase 2**, when\nthe\nnon-factory components (admin server, controller, `dfctl`, memory\nlimiter) are\nactually annotated and their synthetic-URN scheme is settled with the\nSIG.\n- **Per-signal stability** attribute: intentionally **omitted** (a\n`TODO(stability)` documents this). Per the SIG discussion, stability is\nnot\nmodeled per-signal because many components have no signal type, or\nhandle\nmultiple signal types, so a single per-signal stability field doesn't\nfit.\n- **Attribute value validation** (RFC Option C) and the `xtask`\n  `component-inventory` command are later phases.\n\n## What issue does this PR close?\n\nRelates to #3435 (Phase 1 of 4). Does not close it — Phases 2–4 follow.\n\n## How are these changes tested?\n\n`cargo xtask check` (structure, fmt, `clippy --workspace --all-targets\n-D\nwarnings`, `test --workspace`) passes, plus `python3\ntools/sanitycheck.py` for\nthe changelog YAML. New unit and end-to-end tests included.\n\n## Are there any user-facing changes?\n\nAdds new public API (`otap_df_engine::inventory`, the\n`#[component_inventory]`\nmacro) but changes no existing runtime, API, or config behavior. A\n`.chloggen`\nentry is included.\n\n### Changelog\n\n- [x] Added a `.chloggen/*.yaml` entry",
          "timestamp": "2026-07-27T22:09:54Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/537d4ecc1d18287999b4e8573ac2aa29ec447e5c"
        },
        "date": 1785217165933,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.44,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.97,
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
          "id": "33e8e7dead37ed702c18a0c651b7030b84c09a32",
          "message": "OAuth 2.0 Client Auth Extension design doc (#3571)\n\n# Change Summary\n\nAdds a design doc for a proposed OAuth 2.0 Client Auth extension\n(urn:otel:extension:oauth2_client_auth) — the generic, provider-neutral\ncounterpart to the Azure Identity Auth extension, modeled on the Go\ncollector's oauth2clientauthextension.\n\nThe extension acquires and background-refreshes OAuth 2.0 access tokens\n(client-credentials + JWT-bearer grants) and exposes them to data-path\nnodes through the existing `BearerTokenProvider` capability — so the\nOTLP exporters can inject a refreshed Authorization: Bearer header\nwithout embedding static credentials or doing token work on the hot\npath.\n\nKey design points covered:\n\n- Reuses the existing `BearerTokenProvider` capability (no new\ncapability machinery).\n- `Active + Shared` execution, watch-based token cache, slow-path\ncoalescing via `fetch_lock`, background refresh with expiry_buffer,\njittered scheduling + bounded exponential-backoff retry.\n- Readiness-gated startup (blocks data-path spawn until the first token\nis published, bounded by `startup_timeout`.\n- Token-endpoint TLS via the shared\n`otap_df_config::tls::TlsClientConfig`; custom CA, mTLS client cert,\nSNI.\n- Config schema, telemetry, lifecycle, security/performance\nconsiderations, validation expectations, and open questions.\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Related to #3479\n\n## How are these changes tested?\n\nn/a\n\n## Are there any user-facing changes?\n\nno\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [x] This is a documentation-only PR.",
          "timestamp": "2026-07-28T23:50:38Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/33e8e7dead37ed702c18a0c651b7030b84c09a32"
        },
        "date": 1785295495716,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.44,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.97,
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
          "id": "0909afeb86bb385afaeb146b5b86d470aad6ed28",
          "message": "fix(go): Avoid panicking when a batch has > u16 max records (#3615)\n\n# Change Summary\n\nBuilding arrow record batches can panic when the number of input records\nis too large as seen in the attached issue. This PR does not implement\nsupport for larger batches but at least avoids the panic and produces a\nmore descriptive error noting the limitation.\n\n## What issue does this PR close?\n\n* Closes #1883\n\n## How are these changes tested?\n\nUnit\n\n## Are there any user-facing changes?\n\nNew error handling behavior.\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-29T23:22:47Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/0909afeb86bb385afaeb146b5b86d470aad6ed28"
        },
        "date": 1785382421697,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.97,
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
          "id": "75e0ce7f9fd562eb66464293fd651e559e0d0b64",
          "message": "chore(changelog): Tighten breaking change 'Migration' enforcement (#3612)\n\n# Change Summary\n\nReceived some offline feedback that breaking change line items need to\nbe more specific about what action is required from a consumer.\n\nI had separately noticed that the Changelog can get very verbose:\nhttps://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/CHANGELOG.md\n\nThis PR proposes:\n- Every `breaking` changelog entry MUST have a `subtext` starting with\n`Migration:`\n- Limit the `note` to 200 characters and `subtext` to 300 characters\n\nI am hoping the limits will help force authors to carefully choose\nwordings in changelog entries. I did have to touch basically all\nexisting changelog entries to make this new validation pass - please\nfeel free to point out places where my summarization lost accuracy.\n\n## What issue does this PR close?\n\nSemi-related to #3286\n\n## How are these changes tested?\n\nCI runs\n\n## Are there any user-facing changes?\n\nAffects repo-wide PR changelog enforcement\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-07-30T19:30:58Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/75e0ce7f9fd562eb66464293fd651e559e0d0b64"
        },
        "date": 1785467800237,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.97,
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
          "id": "369d5de7684e87ce3fbee4b0a3ee5068eac73aff",
          "message": "feat(metrics): refactor retry processor telemetry (#3544)\n\n# Change Summary\n\nRefactor the retry processor's internal telemetry to use bounded\nenum-based\nattributes and align its metrics with the engine-owned node telemetry\nmodel.\n\nThis PR:\n\n- Replaces `retry_attempts_{signal}` with operationally precise metrics:\n  - `processor.retry.retries.scheduled{signal}`\n  - `processor.retry.requests.recovered{signal}`\n- Adds `processor.retry.requests.terminated{signal, reason}` with\nbounded termination reasons\n- Keeps termination metrics in a separate metric set so `reason` is not\nattached to unrelated operational metrics.\n- Removes duplicated consumed/produced item counters and the\ncorresponding\n`num_items` retry state. Item and message outcomes are already recorded\nby\n  the engine-owned node consumer and producer metrics.\n- Records a retry as scheduled only after the local scheduler accepts\nit.\n- Records a request as recovered when it is acknowledged after one or\nmore\n  retries.\n- Reports the deadline reason when both the deadline and retry-count\nguards\napply, while retaining the retry limit as protection against a stalled\nclock.\n- Forwards local scheduling failures upstream after removing the retry\nframe,\n  preventing them from being routed back into the retry processor.\n- Converts channel send failures that retain their payload into\nretryable\n  NACKs.\n- Updates the retry processor documentation and expands test coverage\nfor the\n  new metric and control-flow semantics.\n\n## What issue does this PR close?\n\n* Related to #3530\n\n## How are these changes tested?\n\nAdded additional tests\n\n## Are there any user-facing changes?\n\nYes. This is a breaking change to the retry processor's internal\ntelemetry\nschema. Existing per-signal metric names are replaced with bounded\nattributes,\nand retry-specific operational metrics are added. Consumers of the\nprevious\nretry metric names will need to update their queries and dashboards.\n\nLocal retry scheduling failures are also now forwarded upstream instead\nof\nbeing routed back to the retry processor.\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: Drew Relmas <drewrelmas@gmail.com>",
          "timestamp": "2026-07-31T22:39:51Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/369d5de7684e87ce3fbee4b0a3ee5068eac73aff"
        },
        "date": 1785554731231,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 98.97,
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
          "id": "a956ea9c21e62d7e83b77ccf3604394ac440ca5c",
          "message": "Refactor transform processor internal telemetry (#3620)\n\n# Change Summary\n\n- Replace transform processor message counters with operation outcome\nand failure metric sets.\n- Reuse bounded shared `signal` and `outcome` attributes and add bounded\n`language` and `error.type` attributes.\n- Classify conversion, query, routing, capacity, send, and internal\nfailures.\n- Document the metric contract and migration from the removed counters.\n\n## What issue does this PR close?\n\nContributes to #3530.\n\n## How are these changes tested?\n\n- Focused transform processor tests\n- `cargo xtask check`\n- Changelog note and subtext length controls\n\n## Are there any user-facing changes?\n\nYes. This is a breaking internal-metric change. Consumers must replace\n`msgs_transformed` and `msgs_transform_failed` queries with\n`processor.transform.operations` and `processor.transform.failures`,\nusing their bounded attributes.\n\n### Changelog\n\n- [x] Added a `.chloggen/*.yaml` entry\n- [ ] This PR is a `chore` (indicated in title)\n- [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: Copilot Autofix powered by AI <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-08-01T07:53:01Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a956ea9c21e62d7e83b77ccf3604394ac440ca5c"
        },
        "date": 1785640553670,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.03,
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
          "id": "a956ea9c21e62d7e83b77ccf3604394ac440ca5c",
          "message": "Refactor transform processor internal telemetry (#3620)\n\n# Change Summary\n\n- Replace transform processor message counters with operation outcome\nand failure metric sets.\n- Reuse bounded shared `signal` and `outcome` attributes and add bounded\n`language` and `error.type` attributes.\n- Classify conversion, query, routing, capacity, send, and internal\nfailures.\n- Document the metric contract and migration from the removed counters.\n\n## What issue does this PR close?\n\nContributes to #3530.\n\n## How are these changes tested?\n\n- Focused transform processor tests\n- `cargo xtask check`\n- Changelog note and subtext length controls\n\n## Are there any user-facing changes?\n\nYes. This is a breaking internal-metric change. Consumers must replace\n`msgs_transformed` and `msgs_transform_failed` queries with\n`processor.transform.operations` and `processor.transform.failures`,\nusing their bounded attributes.\n\n### Changelog\n\n- [x] Added a `.chloggen/*.yaml` entry\n- [ ] This PR is a `chore` (indicated in title)\n- [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: Copilot Autofix powered by AI <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-08-01T07:53:01Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a956ea9c21e62d7e83b77ccf3604394ac440ca5c"
        },
        "date": 1785727054905,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.03,
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
          "id": "b5ef6234f870f5818eaf9cd216824d0bc4d75695",
          "message": "chore(deps): bump aiohttp from 3.14.1 to 3.14.3 in /tools/pipeline_perf_test/orchestrator (#3655)\n\nBumps [aiohttp](https://github.com/aio-libs/aiohttp) from 3.14.1 to\n3.14.3.\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/aio-libs/aiohttp/blob/master/CHANGES.rst\">aiohttp's\nchangelog</a>.</em></p>\n<blockquote>\n<h1>3.14.3 (2026-07-22)</h1>\n<h2>Bug fixes</h2>\n<ul>\n<li>\n<p>Fixed the client dropping only the first <code>Authorization</code>,\n<code>Cookie</code> and\n<code>Proxy-Authorization</code> header when a redirect crossed an\norigin -- by :user:<code>arshsmith1</code>.</p>\n<p><em>Related issues and pull requests on GitHub:</em>\n:issue:<code>13180</code>.</p>\n</li>\n<li>\n<p>Fixed error message construction in the C HTTP parser -- by\n:user:<code>bdraco</code>.</p>\n<p><em>Related issues and pull requests on GitHub:</em>\n:issue:<code>13222</code>.</p>\n</li>\n</ul>\n<hr />\n<h1>3.14.2 (2026-07-20)</h1>\n<h2>Bug fixes</h2>\n<ul>\n<li>\n<p>Fixed :py:attr:<code>~aiohttp.web.StreamResponse.last_modified</code>\nrounding a\n:class:<code>datetime.datetime</code> with a fractional second down.</p>\n<p><em>Related issues and pull requests on GitHub:</em>\n:issue:<code>5303</code>.</p>\n</li>\n<li>\n<p>Fixed resolving <code>localhost</code> on Windows to fall back\nwithout <code>AI_ADDRCONFIG</code>\nwhen the first lookup fails, so <code>localhost</code> still works\nwithout an active\nnetwork.</p>\n<p><em>Related issues and pull requests on GitHub:</em>\n:issue:<code>5357</code>.</p>\n</li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/5e392ce0456f5235a4ee6ad46f0e806df2f15873\"><code>5e392ce</code></a>\nRelease v3.14.3 (<a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13225\">#13225</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/49f65d54150397892f7bcc4aae887767d51c322d\"><code>49f65d5</code></a>\n[PR <a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13222\">#13222</a>/f4866933\nbackport][3.14] Build C parser error message from bounded...</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/240099e5216a01b32919dcd8dd5c6c0b1bf83671\"><code>240099e</code></a>\n[PR <a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13180\">#13180</a>/ee53d655\nbackport][3.14] drop every copy of credential headers on ...</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/d93f30a302f8b930074fe14a2be7b2088ba28111\"><code>d93f30a</code></a>\nBump version (<a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13202\">#13202</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/c1b9212ad3d93c24b5fc66ad0849597166bc816e\"><code>c1b9212</code></a>\nRelease v3.14.2 (<a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13201\">#13201</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/380d4b55e8df48dfd62f1addfb530426f6bc4106\"><code>380d4b5</code></a>\n[PR <a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13054\">#13054</a>/ed8b040c\nbackport][3.14] escape backslashes in digest auth quoted-...</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/e1e1bee363dfba04a9a75c8801717da2ed5bdcb9\"><code>e1e1bee</code></a>\nMake llhttp method array size dynamic (<a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13174\">#13174</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13196\">#13196</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/aa4cf29b6a5ad6f4d21fa1dd3f69193dc2f5d505\"><code>aa4cf29</code></a>\n[PR <a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13170\">#13170</a>/2b906869\nbackport][3.14] Fix StreamResponse.last_modified rounding...</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/71b57b40d85a0723c92b0a5a37ebdf518210d2ea\"><code>71b57b4</code></a>\n[PR <a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13172\">#13172</a>/a57747ed\nbackport][3.14] Fix C parser folding fragment into query_...</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiohttp/commit/64a03fb620b623e5a5a1b7103c07ae3e536a0d40\"><code>64a03fb</code></a>\n[PR <a\nhref=\"https://redirect.github.com/aio-libs/aiohttp/issues/13169\">#13169</a>/1adc0cd7\nbackport][3.14] Upgrade http:// to https:// in README.rst...</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/aio-libs/aiohttp/compare/v3.14.1...v3.14.3\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=aiohttp&package-manager=pip&previous-version=3.14.1&new-version=3.14.3)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\nYou can disable automated security fix PRs for this repo from the\n[Security Alerts\npage](https://github.com/open-telemetry/otel-arrow/network/alerts).\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-08-03T23:28:42Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b5ef6234f870f5818eaf9cd216824d0bc4d75695"
        },
        "date": 1785814939778,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.03,
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
          "id": "2d41c4936861dc57023f865da994e00a4d6229bb",
          "message": "[query-engine] Misc improvements in Value code (#3656)\n\nRelates to #3554\n\n# Changes\n\n* Add a fast path for int\\int and double\\double comparison\n* Move diagnostic code from RecordSet Engine into `Value` so that\nColumnar Engine can reuse it\n\n---------\n\nCo-authored-by: albertlockett <a.lockett@f5.com>\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>",
          "timestamp": "2026-08-05T00:16:35Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2d41c4936861dc57023f865da994e00a4d6229bb"
        },
        "date": 1785902161894,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-binary-size",
            "value": 111.58,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.1,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "manish_vit@hotmail.com",
            "name": "Manish Goel",
            "username": "manishgoel3"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "538053869042e3e6a0566d6bd300010accca6872",
          "message": "feat(geneva-exporter): support agent-fed credentials (#3579)\n\n# Change Summary\n\nAdds agent-fed authentication to the Geneva exporter. An embedding host\nsupplies one atomic token-and-routing snapshot through the\n`agent_fed_credential_provider` capability, bypassing the Geneva Config\nService handshake.\n\nEach upload reads one immutable snapshot, preventing token, endpoint,\nand moniker generations from being mixed during rotation. Invalid,\nunavailable, or near-expiry credentials fail closed.\n\nThe exporter validates that:\n\n- The endpoint is an absolute HTTPS URL without credentials, query, or\nfragment.\n- The moniker matches the configured account or an explicit `default`.\n- Monikers contain only URL-unreserved ASCII characters.\n- Tokens with known expiry remain valid for more than 30 seconds.\n\nToken zeroization is preserved through the merged\n[geneva-uploader\nhardening](https://github.com/open-telemetry/opentelemetry-rust-contrib/pull/716).\n\n## What issue does this PR close?\n\n* Closes #3275 \n\n## How are these changes tested?\n\n- All 77 Geneva exporter unit and regression tests pass.\n- All 23 engine authentication capability tests pass.\n- Engine and contrib-nodes all-target clippy pass with warnings denied.\n- Formatting, Markdown lint, sanity, and focused dependency checks pass.\n- Coverage includes capability binding, atomic rotation, routing\nprecedence, endpoint validation, expiry handling, recovery, existing\nauthentication modes, logs/spans routing and ACK/NACK behavior.\n\n## Are there any user-facing changes?\n\nYes. Geneva exporters can select `auth.type: agentfed` and bind one\ncapability:\n\n```yaml\ncapabilities: agent_fed_credential_provider: agent-auth\nconfig: account: my-account auth:\n    type: agentfed\n```\n\nThe host extension must provide this capability using the shared\nexecution model.\nendpoint region are  are optional in agent-fed mode because the endpoint\ncomes from the credential snapshot. For other authentication modes they\nremain required, and blank values now fail during configuration\nvalidation.\n\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: Manish Goel <292890742+manishgoel3@users.noreply.github.com>\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-06T00:21:49Z",
          "tree_id": "458baa3159b67b6214e067a599b080c947a04aec",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/538053869042e3e6a0566d6bd300010accca6872"
        },
        "date": 1785985810437,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 80.38,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.44,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.79,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.9,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.88,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 67.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.23,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.28,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 111.72,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.1,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Manish Goel",
            "username": "manishgoel3",
            "email": "manish_vit@hotmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "538053869042e3e6a0566d6bd300010accca6872",
          "message": "feat(geneva-exporter): support agent-fed credentials (#3579)\n\n# Change Summary\n\nAdds agent-fed authentication to the Geneva exporter. An embedding host\nsupplies one atomic token-and-routing snapshot through the\n`agent_fed_credential_provider` capability, bypassing the Geneva Config\nService handshake.\n\nEach upload reads one immutable snapshot, preventing token, endpoint,\nand moniker generations from being mixed during rotation. Invalid,\nunavailable, or near-expiry credentials fail closed.\n\nThe exporter validates that:\n\n- The endpoint is an absolute HTTPS URL without credentials, query, or\nfragment.\n- The moniker matches the configured account or an explicit `default`.\n- Monikers contain only URL-unreserved ASCII characters.\n- Tokens with known expiry remain valid for more than 30 seconds.\n\nToken zeroization is preserved through the merged\n[geneva-uploader\nhardening](https://github.com/open-telemetry/opentelemetry-rust-contrib/pull/716).\n\n## What issue does this PR close?\n\n* Closes #3275 \n\n## How are these changes tested?\n\n- All 77 Geneva exporter unit and regression tests pass.\n- All 23 engine authentication capability tests pass.\n- Engine and contrib-nodes all-target clippy pass with warnings denied.\n- Formatting, Markdown lint, sanity, and focused dependency checks pass.\n- Coverage includes capability binding, atomic rotation, routing\nprecedence, endpoint validation, expiry handling, recovery, existing\nauthentication modes, logs/spans routing and ACK/NACK behavior.\n\n## Are there any user-facing changes?\n\nYes. Geneva exporters can select `auth.type: agentfed` and bind one\ncapability:\n\n```yaml\ncapabilities: agent_fed_credential_provider: agent-auth\nconfig: account: my-account auth:\n    type: agentfed\n```\n\nThe host extension must provide this capability using the shared\nexecution model.\nendpoint region are  are optional in agent-fed mode because the endpoint\ncomes from the credential snapshot. For other authentication modes they\nremain required, and blank values now fail during configuration\nvalidation.\n\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nCo-authored-by: Manish Goel <292890742+manishgoel3@users.noreply.github.com>\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-06T00:21:49Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/538053869042e3e6a0566d6bd300010accca6872"
        },
        "date": 1785993757557,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 80.38,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.44,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.79,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.9,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.88,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 67.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.23,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.28,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 111.72,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.1,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a8e27f29d859ed987277498d1126603c0e25a10b",
          "message": "feat(metrics): Perform further metrics cleanup on debug_processor (#3650)\n\n# Change Summary\n\nInspired as discussion topic for #3649\n\nFollow-up to #3522\n\n- Continue removal of metrics that overlap with node\n`produced`/`consumed`\n- Propose removal of `.pdata` from metric namespaces\n\nThe connection to #3437 was missed in PR review of #3522. In addition to\nmoving to enum attributes model, should have also recommended removal of\nthe duplicate item and message counters.\n\n## What issue does this PR close?\n\nRelated to #3649 \n\n## How are these changes tested?\n\nUnit tests\n\n## Are there any user-facing changes?\n\nYes, must use node `produced`/`consumed` instead of `debug_processor`\nmetrics for primary signal types.\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-06T04:03:20Z",
          "tree_id": "0036a8019a253d69df9927f313ef9feb2e3ff103",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a8e27f29d859ed987277498d1126603c0e25a10b"
        },
        "date": 1786002561026,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 80.53,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.44,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.9,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.89,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.08,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.53,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.22,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.29,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 111.91,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.35,
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
          "id": "dab00a4e5a3124ebc1f0c4d24f895454155a0aec",
          "message": "feat: add endpoint to dump heap allocation profile (#3497)\n\n# Change Summary\n\n<!--Replace with a brief summary of the change in this PR-->\n\nAdds an endpoint (/api/v1/debug/pprof/heap) that can dump the a profile\n(in pprof format) heap allocations.\n\nFurther reading about this solution:\nhttps://www.polarsignals.com/blog/posts/2023/12/20/rust-memory-profiling\n\nRun the app w/ profiling enabled. `jemalloc` must be the allocator\n(default on linux and macos):\n```sh\n_RJEM_MALLOC_CONF=\"prof:true,prof_active:true,lg_prof_sample:19\" \\\ncargo run -- \\\n --config ./configs/trafficgen-filter-debug-noop.yaml\n```\n\nscrape the endpoint, open in your favourite pprof viewer\n```\ncurl -XGET http://localhost:8080/api/v1/debug/pprof/heap -o out.pprof\ngo tool pprof -http=:18080 ./target/debug/df_engine ./out.pprof\n```\n\n<img width=\"1724\" height=\"704\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/43f43503-05e9-44c4-960b-39fc664bf36e\"\n/>\n\n\nThis endpoint will return 500 when:\n- jemalloc is not the allocator, or jemalloc is used, but was compiled\nwithout the `profiling` feature\n- if the jemalloc options `prof:true,prof_active:true` are not set\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Closes #3660\n\n## How are these changes tested?\n\nManually\n\n## Are there any user-facing changes?\n\n <!-- If yes, provide further info below -->\n \n Yes, the user could call this endpoint\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-07T00:16:10Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/dab00a4e5a3124ebc1f0c4d24f895454155a0aec"
        },
        "date": 1786071871559,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 80.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.46,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.75,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 112.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "cijo.thomas@gmail.com",
            "name": "Cijo Thomas",
            "username": "cijothomas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1a7bd6347bf423e6694c33490fab6f7930f90498",
          "message": "chore(opamp): improve reconciliation diagnostics and example (#3676)\n\n## Summary\n\nAdd local OpAMP lifecycle logs for WebSocket connection and remote\nconfiguration reconciliation, including complete failure reasons.\n\nMake the OpAMP controller example self-contained by generating\nlightweight log traffic and forwarding it through a local OTLP loopback\npipeline. This makes it relatively easy to test.",
          "timestamp": "2026-08-07T01:02:29Z",
          "tree_id": "31199babfa6643e82d9af6c5b8feb9bc27cfe4d5",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1a7bd6347bf423e6694c33490fab6f7930f90498"
        },
        "date": 1786088659561,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.46,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.75,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.44,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.22,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 112.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lalit_fin@yahoo.com",
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ca4ad7ef5cce0ae7472d69df8a141acf49252692",
          "message": "  chore: fix non-jemalloc test feature guard (#3690)\n\n# Change Summary\n\nFix the non-jemalloc pipeline metrics test gate so the test only runs\nwhen jemalloc accounting is unavailable.\n\nThe issue was exposed by #3497 enabling jemalloc's\n`unprefixed_malloc_on_supported_platforms` feature. The test then\nobserved real jemalloc allocations while incorrectly expecting zero.\n\n  ## What issue does this PR close?\n\n  * Follow-up to #3497\n\n  ## How are these changes tested?\n\n  - `cargo fmt --all -- --check`\n  - `python3 tools/sanitycheck.py`\n  - `cargo check -p otap-df-engine`\n  - Full configuration coverage will run in CI.\n\n  ## Are there any user-facing changes?\n\n  No. This only corrects test selection.\n\n  ### Changelog\n\n  * [ ] Added a `.chloggen/*.yaml` entry\n  * [x] This PR is a `chore` (indicated in title)\n  * [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-07T10:44:07Z",
          "tree_id": "edf1e4195340c1fb9959b71fd4c0068864865cb7",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ca4ad7ef5cce0ae7472d69df8a141acf49252692"
        },
        "date": 1786112306971,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.46,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.75,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.44,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 112.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "161134993+Dipanshusinghh@users.noreply.github.com",
            "name": "Dipanshu singh",
            "username": "Dipanshusinghh"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8d38d41cd89a7c3db8c271f1a4ec392eb171fe8e",
          "message": "fix(metrics): include custom identity attributes in channel metrics (#3647)\n\nFixes #3645\n\nThis PR addresses the issue where custom telemetry identity attributes\nconfigured via `entity.extend.identity_attributes` were dropped from\nchannel-backed metrics (e.g., `produced.items`).\n\n**Changes**\n* Introduced `NodeWithCustomChannelAttributeSet` to carry custom\nattributes alongside the base channel attributes.\n* Updated `PipelineContext::register_node_channel_entity` to construct\n`NodeWithCustomChannelAttributeSet` when custom attributes are present\non the node.\n\nThis ensures that producers and consumers correctly see the configured\ncomponent identity on channel metrics, bringing them into parity with\nstandard node-local metrics and logs.",
          "timestamp": "2026-08-07T16:26:49Z",
          "tree_id": "6a9a734a5d85f7f9ef3ea70a8226e32362a31c5d",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8d38d41cd89a7c3db8c271f1a4ec392eb171fe8e"
        },
        "date": 1786132364517,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.47,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.75,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 112.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 99.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pritishnahar@gmail.com",
            "name": "Pritish Nahar",
            "username": "pritishnahar95"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "30bb3bcf917814b46140bb48a008acb0953cdee9",
          "message": "doc: make auth extension READMEs the full config reference (#3696)\n\n# Change Summary\n\nThe oauth2_client_auth and azure_identity_auth READMEs only summarized\nthe extensions and deferred to design.md, so operators had to read\ndesign docs or consumer exporter READMEs to find configuration options.\nConsumer READMEs in turn duplicated provider details that drift out of\nsync.\n\nRewrite both extension READMEs as standalone usage guides: metadata,\ngetting started, build/feature gates, complete config field tables\n(including grant-specific and method-specific fields), validation rules,\ntelemetry, and troubleshooting. Design rationale and lifecycle detail\nstay in design.md.\n\nRemove the duplicated provider configuration from the Azure Monitor and\nOTLP HTTP exporter READMEs, which now document only the capability\nbinding and link to the extension. Update the contrib-extensions catalog\nto link usage and design docs and state the ownership rule.\n\n## What issue does this PR close?\n\n* Related to #3479 \n* Related to #3356\n\n## How are these changes tested?\n\nn/a\n\n## Are there any user-facing changes?\n\nn/a\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [x] This is a documentation-only PR.",
          "timestamp": "2026-08-07T22:28:56Z",
          "tree_id": "35a8cf636a56af20a96f76ed366d890a8b7db980",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/30bb3bcf917814b46140bb48a008acb0953cdee9"
        },
        "date": 1786154716935,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.39,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.01,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
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
          "id": "30bb3bcf917814b46140bb48a008acb0953cdee9",
          "message": "doc: make auth extension READMEs the full config reference (#3696)\n\n# Change Summary\n\nThe oauth2_client_auth and azure_identity_auth READMEs only summarized\nthe extensions and deferred to design.md, so operators had to read\ndesign docs or consumer exporter READMEs to find configuration options.\nConsumer READMEs in turn duplicated provider details that drift out of\nsync.\n\nRewrite both extension READMEs as standalone usage guides: metadata,\ngetting started, build/feature gates, complete config field tables\n(including grant-specific and method-specific fields), validation rules,\ntelemetry, and troubleshooting. Design rationale and lifecycle detail\nstay in design.md.\n\nRemove the duplicated provider configuration from the Azure Monitor and\nOTLP HTTP exporter READMEs, which now document only the capability\nbinding and link to the extension. Update the contrib-extensions catalog\nto link usage and design docs and state the ownership rule.\n\n## What issue does this PR close?\n\n* Related to #3479 \n* Related to #3356\n\n## How are these changes tested?\n\nn/a\n\n## Are there any user-facing changes?\n\nn/a\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [x] This is a documentation-only PR.",
          "timestamp": "2026-08-07T22:28:56Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/30bb3bcf917814b46140bb48a008acb0953cdee9"
        },
        "date": 1786156573677,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.39,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.01,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
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
          "id": "30bb3bcf917814b46140bb48a008acb0953cdee9",
          "message": "doc: make auth extension READMEs the full config reference (#3696)\n\n# Change Summary\n\nThe oauth2_client_auth and azure_identity_auth READMEs only summarized\nthe extensions and deferred to design.md, so operators had to read\ndesign docs or consumer exporter READMEs to find configuration options.\nConsumer READMEs in turn duplicated provider details that drift out of\nsync.\n\nRewrite both extension READMEs as standalone usage guides: metadata,\ngetting started, build/feature gates, complete config field tables\n(including grant-specific and method-specific fields), validation rules,\ntelemetry, and troubleshooting. Design rationale and lifecycle detail\nstay in design.md.\n\nRemove the duplicated provider configuration from the Azure Monitor and\nOTLP HTTP exporter READMEs, which now document only the capability\nbinding and link to the extension. Update the contrib-extensions catalog\nto link usage and design docs and state the ownership rule.\n\n## What issue does this PR close?\n\n* Related to #3479 \n* Related to #3356\n\n## How are these changes tested?\n\nn/a\n\n## Are there any user-facing changes?\n\nn/a\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [x] This is a documentation-only PR.",
          "timestamp": "2026-08-07T22:28:56Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/30bb3bcf917814b46140bb48a008acb0953cdee9"
        },
        "date": 1786241524745,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.39,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.01,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "104617579+Shaurya2k06@users.noreply.github.com",
            "name": "Shaurya Srivastava",
            "username": "Shaurya2k06"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e19f973a1001c6d1a0f7204d83d3b087257076df",
          "message": "chore(ci): add offline markdown link checker (#3594)\n\n# Change Summary\n\nAdd a Repo Lint lychee job that fails PRs on broken relative Markdown\nlinks and anchors across the whole tree (not just changed files). Fix\nthe three existing broken relative links so the check is green.\n\n## What issue does this PR close?\n\n* Closes #3580\n\n## How are these changes tested?\n\n- Ran lychee offline locally on all `**/*.md` (0 errors after link\nfixes)\n- markdownlint + sanitycheck on touched Markdown\n\n## Are there any user-facing changes?\n\nNo. CI + doc link fixes only.\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nSigned-off-by: shaurya2k06 <shaurya2k06@gmail.com>",
          "timestamp": "2026-08-09T13:27:45Z",
          "tree_id": "93f4c9389668ae82f4862c65fdb7966e6c821d41",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/e19f973a1001c6d1a0f7204d83d3b087257076df"
        },
        "date": 1786304744379,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.39,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.01,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Shaurya Srivastava",
            "username": "Shaurya2k06",
            "email": "104617579+Shaurya2k06@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "e19f973a1001c6d1a0f7204d83d3b087257076df",
          "message": "chore(ci): add offline markdown link checker (#3594)\n\n# Change Summary\n\nAdd a Repo Lint lychee job that fails PRs on broken relative Markdown\nlinks and anchors across the whole tree (not just changed files). Fix\nthe three existing broken relative links so the check is green.\n\n## What issue does this PR close?\n\n* Closes #3580\n\n## How are these changes tested?\n\n- Ran lychee offline locally on all `**/*.md` (0 errors after link\nfixes)\n- markdownlint + sanitycheck on touched Markdown\n\n## Are there any user-facing changes?\n\nNo. CI + doc link fixes only.\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.\n\n---------\n\nSigned-off-by: shaurya2k06 <shaurya2k06@gmail.com>",
          "timestamp": "2026-08-09T13:27:45Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/e19f973a1001c6d1a0f7204d83d3b087257076df"
        },
        "date": 1786328227571,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.39,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.01,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "29139614+renovate[bot]@users.noreply.github.com",
            "name": "renovate[bot]",
            "username": "renovate[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cfda6b46cb336af4ea1c62c84040d793f2f46d69",
          "message": "chore(deps): update opentelemetry-resource-detectors digest to 24d1f73 (#3702)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| opentelemetry-resource-detectors | workspace.dependencies | digest |\n`4f5296b` → `24d1f73` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on Monday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0NC4xMi4wIiwidXBkYXRlZEluVmVyIjoiNDQuMTIuMCIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-08-10T12:14:31Z",
          "tree_id": "68de44c819d6ac3434847007c8290f9346dae8fc",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/cfda6b46cb336af4ea1c62c84040d793f2f46d69"
        },
        "date": 1786369458794,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.39,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.01,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.35,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "29139614+renovate[bot]@users.noreply.github.com",
            "name": "renovate[bot]",
            "username": "renovate[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f507031c5004de0c6269d0f8522b4e5f8821d48a",
          "message": "chore(deps): update weaver crates to f0b3b36 (#3703)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| weaver_common | workspace.dependencies | digest | `885d083` →\n`f0b3b36` |\n| weaver_forge | workspace.dependencies | digest | `885d083` → `f0b3b36`\n|\n| weaver_resolved_schema | workspace.dependencies | digest | `885d083` →\n`f0b3b36` |\n| weaver_resolver | workspace.dependencies | digest | `885d083` →\n`f0b3b36` |\n| weaver_semconv | workspace.dependencies | digest | `885d083` →\n`f0b3b36` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on Monday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about these\nupdates again.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0NC4xMi4wIiwidXBkYXRlZEluVmVyIjoiNDQuMTIuMCIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-08-10T18:17:45Z",
          "tree_id": "b76d8173cc114e203e8354da33c120d15d1bf52e",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f507031c5004de0c6269d0f8522b4e5f8821d48a"
        },
        "date": 1786391263294,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.38,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.82,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.22,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.41,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "cijo.thomas@gmail.com",
            "name": "Cijo Thomas",
            "username": "cijothomas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c190552e0036de9e9a4ce7b98d2dbbc9681927e3",
          "message": "feat(telemetry): reload internal log levels during reconciliation (#3613)\n\n# Change Summary\n\nChanging internal log verbosity previously required restarting the\nengine, which discards the in-process state an operator is usually\ntrying to inspect.\n\nExtends `df_engine` full-engine reconciliation to apply changes to\n`engine.telemetry.logs.level` to existing tracing subscribers. Internal\nlog level can now be changed dynamically through the admin control plane\nor OpAMP, without restarting the engine.\n\n```yaml\nengine:\n  telemetry:\n    logs:\n      level: warn\n```\n\nUpdate flow: Admin or OpAMP submits a full desired config -> the\ncontroller validates and reconciles it -> on success, every live tracing\ndispatcher atomically receives the new filter -> tracing rebuilds its\ncallsite-interest cache so the new level takes effect immediately.\nFailed reconciliation keeps the previous filter.\n\nReconciling the same configuration with a different `level` applies the\nnew value to every live tracing dispatcher.\n\nEach tracing dispatcher retains its own `EnvFilter` instance, so filter\nstate is never shared across engine threads. A shared update registry\nreplaces every live dispatcher filter only after successful\nreconciliation and rebuilds tracing's callsite-interest cache so both\nverbosity increases and decreases take effect. Failed reconciliation\npreserves the active filter, and reconciliations that do not change\n`logs.level` skip filter parsing and cache rebuilding.\n\nA valid `RUST_LOG` value supplies the startup filter. After startup, a\nsuccessful reconciliation makes `engine.telemetry.logs.level`\nauthoritative, allowing OpAMP and admin updates to replace the\nenvironment-derived filter.\n\nOnly `engine.telemetry.logs.level` is applied to running components.\nOther `engine.telemetry` fields are recorded in the controller's live\nconfiguration but are not applied at runtime; extending live reload to\nmore settings is left for follow-up work.\n\n## Performance\n\n`df_engine` does not compile out debug or trace callsites today;\ndisabled callsites are filtered at runtime and cached as uninterested.\nThis change preserves that steady-state behavior. Benchmarks comparing\nthe static `EnvFilter` with the reloadable filter (committed in\n`benches/self_tracing/main.rs`) show disabled logs unchanged at the ~1\nns cached-interest floor, and enabled logs ~1.2% slower (135.3 ns to\n136.9 ns) from the added indirection. Both figures assume level and\ntarget directives; span directives disable the cached-interest floor.\nBuilds that use tracing's compile-time max-level features cannot\ndynamically enable callsites that were removed during compilation.\n\n## What issue does this PR close?\n\nRelates to #3387\n\n## Are there any user-facing changes?\n\nYes. Internal log severity and target directives can be changed through\nsuccessful full-engine reconciliation without restarting `df_engine`.\n\n### Changelog\n\n- [x] Added a `.chloggen/*.yaml` entry\n- [ ] This PR is a `chore` (indicated in title)\n- [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-11T00:01:53Z",
          "tree_id": "1bc8bbac7e05fb4bdc635093e864258663d4c334",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c190552e0036de9e9a4ce7b98d2dbbc9681927e3"
        },
        "date": 1786412748791,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.44,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.5,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.88,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.07,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
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
          "id": "c190552e0036de9e9a4ce7b98d2dbbc9681927e3",
          "message": "feat(telemetry): reload internal log levels during reconciliation (#3613)\n\n# Change Summary\n\nChanging internal log verbosity previously required restarting the\nengine, which discards the in-process state an operator is usually\ntrying to inspect.\n\nExtends `df_engine` full-engine reconciliation to apply changes to\n`engine.telemetry.logs.level` to existing tracing subscribers. Internal\nlog level can now be changed dynamically through the admin control plane\nor OpAMP, without restarting the engine.\n\n```yaml\nengine:\n  telemetry:\n    logs:\n      level: warn\n```\n\nUpdate flow: Admin or OpAMP submits a full desired config -> the\ncontroller validates and reconciles it -> on success, every live tracing\ndispatcher atomically receives the new filter -> tracing rebuilds its\ncallsite-interest cache so the new level takes effect immediately.\nFailed reconciliation keeps the previous filter.\n\nReconciling the same configuration with a different `level` applies the\nnew value to every live tracing dispatcher.\n\nEach tracing dispatcher retains its own `EnvFilter` instance, so filter\nstate is never shared across engine threads. A shared update registry\nreplaces every live dispatcher filter only after successful\nreconciliation and rebuilds tracing's callsite-interest cache so both\nverbosity increases and decreases take effect. Failed reconciliation\npreserves the active filter, and reconciliations that do not change\n`logs.level` skip filter parsing and cache rebuilding.\n\nA valid `RUST_LOG` value supplies the startup filter. After startup, a\nsuccessful reconciliation makes `engine.telemetry.logs.level`\nauthoritative, allowing OpAMP and admin updates to replace the\nenvironment-derived filter.\n\nOnly `engine.telemetry.logs.level` is applied to running components.\nOther `engine.telemetry` fields are recorded in the controller's live\nconfiguration but are not applied at runtime; extending live reload to\nmore settings is left for follow-up work.\n\n## Performance\n\n`df_engine` does not compile out debug or trace callsites today;\ndisabled callsites are filtered at runtime and cached as uninterested.\nThis change preserves that steady-state behavior. Benchmarks comparing\nthe static `EnvFilter` with the reloadable filter (committed in\n`benches/self_tracing/main.rs`) show disabled logs unchanged at the ~1\nns cached-interest floor, and enabled logs ~1.2% slower (135.3 ns to\n136.9 ns) from the added indirection. Both figures assume level and\ntarget directives; span directives disable the cached-interest floor.\nBuilds that use tracing's compile-time max-level features cannot\ndynamically enable callsites that were removed during compilation.\n\n## What issue does this PR close?\n\nRelates to #3387\n\n## Are there any user-facing changes?\n\nYes. Internal log severity and target directives can be changed through\nsuccessful full-engine reconciliation without restarting `df_engine`.\n\n### Changelog\n\n- [x] Added a `.chloggen/*.yaml` entry\n- [ ] This PR is a `chore` (indicated in title)\n- [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-11T00:01:53Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c190552e0036de9e9a4ce7b98d2dbbc9681927e3"
        },
        "date": 1786414360337,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.44,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.5,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.76,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.88,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.07,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "68666585+timr-dev@users.noreply.github.com",
            "name": "Tim R",
            "username": "timr-dev"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7d6ec63f00f178b8754a32b1b2cec2ab2c2e0874",
          "message": "fix(journald): bound filtered follow waits (#3674)\n\n## Summary\n\n- bound each journald follow call to one blocking wait so non-matching\njournal activity cannot indefinitely delay batch flushes, checkpoint\ncommands, drain, or shutdown\n- exercise the production `next()` / `wait()` control flow with\nbehavior-level tests for `start_at: end`, idle timeouts, non-matching\nwakes, invalidation, errors, immediate reads, raw-tail stalls, cursor\nadvancement, and current-entry failures\n- emit and document `journald_receiver.start_at_end_head_recovery` from\nthe pipeline thread when fresh `start_at: end` cannot establish a\nmatching tail anchor\n- avoid the libsystemd infinite-wait sentinel and remove one per-record\ncursor clone\n\nRefs #3399.\n\n## Scope\n\nThis PR addresses the behavior-level follow-test work in #3399 and the\nclosely coupled wait-budget defect found by the mandatory review panel.\n\nIt deliberately does **not** close #3399. We are knowingly retaining the\ncurrent best-effort head recovery for fresh `start_at: end`: buggy\nlibsystemd behavior or a later `SD_JOURNAL_INVALIDATE` can expose\npre-startup matching history. The warning and operator documentation\nmake that risk explicit, while #3399 remains open for the dedicated\nmonotonic/boot-aware durable boundary guard. This PR does not claim that\n`start_at: end` is fully replay-safe.\n\nThe diff is larger than a typical test-only change because the\nproduction follow seam, faithful fake state machine, branch/error\ncoverage, operator telemetry, documentation, and changelog form one\nreviewable behavior contract.\n\n## Validation\n\n- `cargo test -p otap-df-core-nodes --lib` (850 tests: 846 passed, 4\nignored)\n- `cargo clippy -p otap-df-core-nodes --lib --tests -- -D warnings`\n- Linux target `cargo-zigbuild check`\n- Linux target test compilation with `cargo-zigbuild test --no-run`\n- `cargo xtask check` with constrained build parallelism\n- `markdownlint-cli2` on both changed Markdown files\n- pinned `chloggen v0.30.0` validation for Go and Rust entries\n- ASCII/LF checks for changed Rust, YAML, and Markdown files\n\n## Review panel\n\nTwenty independent reviews ran across SRE, SDET, Security, Performance\nArchitect, and Adversarial personas using Claude Opus 5, GPT-5.6 Sol,\nGemini 3.1 Pro, and MAI Code Flash. Material findings were deduplicated\nand resolved; MAI findings were advisory and independently verified by\nfrontier models. Resolution re-reviews found no remaining defect in this\nPR's stated scope.",
          "timestamp": "2026-08-11T07:08:08Z",
          "tree_id": "282f237240de111d573bec09e5be36d497c9caa7",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7d6ec63f00f178b8754a32b1b2cec2ab2c2e0874"
        },
        "date": 1786456462213,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.42,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.5,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.74,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.87,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f2b0348342bd6f0aae619c950dfc93b1f1bb1052",
          "message": "feat(metrics): Add durable_buffer.loss bytes metric (#3715)\n\n# Change Summary\n\nAdd `processor.durable_buffer.loss.bytes{reason}` for persisted bytes\nremoved by durable-buffer runtime retention.\n\nThe durable buffer already reports lost segments and bundles by\nretention reason and lost items by reason and signal. Those counts do\nnot show the persisted volume discarded when the buffer applies\nDropOldest or max-age expiry.\n\nBytes are aggregate rather than signal-specific because a segment may\ncontain bundles from multiple signals. The value includes the complete\npersisted file representation, including metadata and encoding overhead.\n\n### Validation\n\nRunning the sample config:\n\n```text\nprocessor.durable_buffer.loss.segments{reason}\nprocessor.durable_buffer.loss.bundles{reason}\nprocessor.durable_buffer.loss.bytes{reason}\nprocessor.durable_buffer.loss.items{reason,signal}\n```\n\nAn unreachable-exporter run exercised both runtime retention paths:\n\n| Pipeline | Traffic | Retention |\n| --- | --- | --- |\n| DropOldest | 50,000 logs/s, batches up to 1,000 | 192 MiB cap with\n`drop_oldest` |\n| Max age | 1,000 logs/s, batches up to 100 | 5-second max age with a\n192 MiB cap |\n\nThe admin accumulator was reset, then sampled and reset again five\nseconds later. Loss values are deltas for that window; storage used and\nutilization are point-in-time gauges at the end of the window.\n\n| Pipeline | Reason | Utilization | Storage used | Segments lost |\nBundles lost | Items lost | Storage lost |\n| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n| DropOldest | `drop_oldest` | 82.66% | 158.70 MiB | 30 | 175 | 175,000\n| 62.73 MiB |\n| Max age | `expired` | 14.54% | 27.92 MiB | 48 | 50 | 5,000 | 2.04 MiB\n|\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Follow-up to:\n  * #3516\n  * #3705\n\n## How are these changes tested?\n\nUnit tests and local engine runs\n\n## Are there any user-facing changes?\n\nYes, added a new `loss.bytes` metric.\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-11T23:33:43Z",
          "tree_id": "7c58fe1419d167099afdb7a2518d570d38c802a6",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f2b0348342bd6f0aae619c950dfc93b1f1bb1052"
        },
        "date": 1786499605446,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.43,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.5,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.74,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.86,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.61,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.41,
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
          "id": "f2b0348342bd6f0aae619c950dfc93b1f1bb1052",
          "message": "feat(metrics): Add durable_buffer.loss bytes metric (#3715)\n\n# Change Summary\n\nAdd `processor.durable_buffer.loss.bytes{reason}` for persisted bytes\nremoved by durable-buffer runtime retention.\n\nThe durable buffer already reports lost segments and bundles by\nretention reason and lost items by reason and signal. Those counts do\nnot show the persisted volume discarded when the buffer applies\nDropOldest or max-age expiry.\n\nBytes are aggregate rather than signal-specific because a segment may\ncontain bundles from multiple signals. The value includes the complete\npersisted file representation, including metadata and encoding overhead.\n\n### Validation\n\nRunning the sample config:\n\n```text\nprocessor.durable_buffer.loss.segments{reason}\nprocessor.durable_buffer.loss.bundles{reason}\nprocessor.durable_buffer.loss.bytes{reason}\nprocessor.durable_buffer.loss.items{reason,signal}\n```\n\nAn unreachable-exporter run exercised both runtime retention paths:\n\n| Pipeline | Traffic | Retention |\n| --- | --- | --- |\n| DropOldest | 50,000 logs/s, batches up to 1,000 | 192 MiB cap with\n`drop_oldest` |\n| Max age | 1,000 logs/s, batches up to 100 | 5-second max age with a\n192 MiB cap |\n\nThe admin accumulator was reset, then sampled and reset again five\nseconds later. Loss values are deltas for that window; storage used and\nutilization are point-in-time gauges at the end of the window.\n\n| Pipeline | Reason | Utilization | Storage used | Segments lost |\nBundles lost | Items lost | Storage lost |\n| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n| DropOldest | `drop_oldest` | 82.66% | 158.70 MiB | 30 | 175 | 175,000\n| 62.73 MiB |\n| Max age | `expired` | 14.54% | 27.92 MiB | 48 | 50 | 5,000 | 2.04 MiB\n|\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Follow-up to:\n  * #3516\n  * #3705\n\n## How are these changes tested?\n\nUnit tests and local engine runs\n\n## Are there any user-facing changes?\n\nYes, added a new `loss.bytes` metric.\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-11T23:33:43Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/f2b0348342bd6f0aae619c950dfc93b1f1bb1052"
        },
        "date": 1786501471652,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.43,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.5,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.74,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.86,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.61,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.41,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "l.querel@f5.com",
            "name": "Laurent Quérel",
            "username": "lquerel"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "48b3bbc76d8522f8891f5ebb53c4a0602a368efc",
          "message": "Introduce `max_in_flight`  in the ClickHouse exporter (#3709)\n\n# Change Summary\n\nAdds configurable, bounded concurrency to the df_engine ClickHouse\nexporter.\n\nThe new `max_in_flight` setting controls how many ClickHouse HTTP insert\nrequests may execute concurrently. It defaults to 10, enabling\nconcurrent inserts without requiring additional configuration.\n\nThe exporter applies backpressure when the configured limit is reached\nand drains accepted requests during shutdown. Concurrent requests may\ncomplete out of order.\n\nUsers who require serialized insertion behavior can set:\n\n```yaml\nexporter:\n  type: urn:otel:exporter:clickhouse\n  config:\n    endpoint: http://clickhouse:8123\n    max_in_flight: 1\n```\n\n## Performance impact\n\nThe change was benchmarked against the serialized implementation using:\n\n- 8,192-log input batches\n- synchronous ClickHouse inserts\n- `max_in_flight: 1` for the baseline\n- `max_in_flight: 10` for this change and the new default\n- one df_engine core\n- six ClickHouse cores\n- twelve traffic-generator cores\n- ClickHouse 25.6\n- three 60-second repetitions per scenario\n\nMedian ClickHouse written throughput under maximum offered load:\n\n| Input path | Serialized baseline | New default | Gain |\n| --- | --- | --- | --- |\n| DFE OTAP | 180,271 logs/s | 682,597 logs/s | +278.6% / 3.79x |\n| DFE OTLP | 179,810 logs/s | 437,543 logs/s | +143.3% / 2.43x |\n\nAt a fixed offered load of approximately 100,000 logs/s, written\nthroughput remained unchanged, as expected. DFE CPU usage decreased by\napproximately 1.0% for OTAP and 9.8% for OTLP.\n\nThese results indicate that the new default can provide up to\napproximately 3.8x OTAP throughput and 2.4x OTLP throughput when\nserialized synchronous inserts are the bottleneck. The actual gain\ndepends on the workload, ClickHouse capacity, and insertion latency.\n\nConcurrent inserts drive ClickHouse harder and increased memory\nconsumption during the saturation runs. Operators can reduce\n`max_in_flight` when ClickHouse resource consumption or insertion\nordering is more important than maximum throughput.\n\n## What issue does this PR close?\n\n- Related to #3512\n- Implements the bounded-concurrency follow-up identified by the\nClickHouse exporter benchmarks in #3512\n\n## How are these changes tested?\n\nAutomated tests cover:\n\n- defaulting `max_in_flight` to 10\n- accepting an explicitly configured concurrency limit\n- rejecting a zero concurrency limit\n- enforcing the configured bound\n- applying backpressure before admitting another request\n- draining all accepted writes during shutdown\n- preserving completed row counts\n\nThe full workspace check passed.\n\n## Are there any user-facing changes?\n\nYes. The ClickHouse exporter now allows up to ten concurrent inserts by\ndefault.\n\nThe new `max_in_flight` positive integer setting can be used to tune\nthis limit. Values greater than one improve throughput by overlapping\nsynchronous HTTP inserts, but inserts may complete out of order and can\nplace more load on ClickHouse.\n\nSet `max_in_flight: 1` to retain serialized insertion behavior.\n\n### Changelog\n\n- [x] Added a `.chloggen/*.yaml` entry\n- [ ] This PR is a chore (indicated in title)\n- [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-12T06:43:42Z",
          "tree_id": "7917b2601c0b7651a012baf1925e081484fe5ae6",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/48b3bbc76d8522f8891f5ebb53c4a0602a368efc"
        },
        "date": 1786542414278,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.43,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.5,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.74,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.86,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.61,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.21,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.41,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lalit_fin@yahoo.com",
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "db03ee3e25ee8529e631fe76af3bbb302e8f148d",
          "message": "  chore(geneva): make certificate authentication opt-in (#3730)\n\n## Summary\n\n  - Update `geneva-uploader` to the latest upstream commit.\n  - Add an opt-in `geneva-certificate-auth` feature.\n  - Keep PKCS#12 certificate authentication disabled by default.\n  - Reject certificate configuration early when the feature is disabled.\n  - Update Geneva documentation, examples, and tests.\n\n  ## Validation\n\n- Focused tests passed with certificate authentication enabled and\ndisabled.\n- Formatting, sanity checks, markdownlint, and changelog validation\npassed.",
          "timestamp": "2026-08-13T00:19:16Z",
          "tree_id": "55bae21fcb69343cfdf04240fc9207698856fb4c",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/db03ee3e25ee8529e631fe76af3bbb302e8f148d"
        },
        "date": 1786585869850,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.43,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.77,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.89,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.6,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.07,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
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
          "id": "db03ee3e25ee8529e631fe76af3bbb302e8f148d",
          "message": "  chore(geneva): make certificate authentication opt-in (#3730)\n\n## Summary\n\n  - Update `geneva-uploader` to the latest upstream commit.\n  - Add an opt-in `geneva-certificate-auth` feature.\n  - Keep PKCS#12 certificate authentication disabled by default.\n  - Reject certificate configuration early when the feature is disabled.\n  - Update Geneva documentation, examples, and tests.\n\n  ## Validation\n\n- Focused tests passed with certificate authentication enabled and\ndisabled.\n- Formatting, sanity checks, markdownlint, and changelog validation\npassed.",
          "timestamp": "2026-08-13T00:19:16Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/db03ee3e25ee8529e631fe76af3bbb302e8f148d"
        },
        "date": 1786590785132,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.43,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.77,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.89,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.6,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.07,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "136855179+ragumarimuthu-git@users.noreply.github.com",
            "name": "Ragu Marimuthu",
            "username": "ragumarimuthu-git"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "00c8f10c341584d2448216f09429285127305d98",
          "message": "fix(pdata): split oversize single OTLP resource entry when batching by bytes (#3673)\n\n## Description\n\nWhen the batch processor batches by bytes (`sizer: bytes`),\n`make_bytes_batches`\npreviously only cut at **top-level resource-entry boundaries**. A single\nOTLP\nrequest carrying one `ResourceLogs` / `ResourceSpans` /\n`ResourceMetrics` entry\nis a single indivisible top-level field, so it was emitted whole\nregardless of\n`max_size` — e.g. 10k log records in one request never split.\n\nThis rewrites the byte splitter to **descend within an oversize resource\nentry**:\nwhole scopes are packed greedily into resource-entry fragments, and when\na single\nscope is still too large it is split by individual records. The resource\nand\nscope wrapper headers (including `schema_url` and unknown fields) are\nre-encoded\naround every fragment so each output batch is a valid\n`ExportXServiceRequest`.\n\nThe path is signal-agnostic — logs, traces and metrics share the same\nOTLP\nnesting (resource entry = field 1; scope list and record list = field\n2), so one\ncode path covers all three.\n\n## Guarantees\n\n- Records are never dropped, duplicated or reordered; unknown wrapper\nfields are\n  preserved.\n- The whole-entry fast path stays byte-exact; splitting only happens for\nentries\n  that exceed `max_size` on their own.\n- Any indivisible unit whose minimal encoding still exceeds `max_size`\n(a lone\nrecord, an opaque/unparseable field, or a wrapper-only fragment) is\nemitted\n  alone, exceeding the limit (best-effort).\n- A malformed nested field aborts sub-splitting and emits the\nentry/scope whole,\nbyte-preserving, rather than reordering the corrupt tail ahead of valid\nrecords.\n\n## Tests\n\nAdds sub-resource splitting tests: single-resource many-records,\nmulti-scope,\nunknown-field preservation, lone-oversize-record, malformed nested\nfields (at\nresource and scope level), plus traces and metrics variants. The logs\nsplit\ntests add an independent **ordered** record comparison (the shared\n`assert_equivalent` canonicalizes into a `BTreeSet`, so it cannot by\nitself catch\nduplication or reordering).\n\n- `cargo test -p otap-df-pdata otlp::batching` → 12 passed\n- `cargo test -p otap-df-core-nodes batch_processor` → 50 passed\n- `cargo clippy -p otap-df-pdata --all-targets -- -D warnings` → clean\n\nFixes #3661\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>\nCopilot-Session: bfc8430d-07c1-49bb-9497-ef6471a3cd7d",
          "timestamp": "2026-08-13T03:17:13Z",
          "tree_id": "5ad1801395115d5b0dfe975bb74811e1da8a4279",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/00c8f10c341584d2448216f09429285127305d98"
        },
        "date": 1786609064161,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.46,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.53,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.8,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.9,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.13,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.54,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mblanchard@macrosssoftware.com",
            "name": "Mikel Blanchard",
            "username": "CodeBlanch"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "251ce3f2cb0a5116798723981474505f32271e4d",
          "message": "[query-engine] Refactor Value::convert_to_regex API (#3744)\n\nRelates to #3608\n\nFollowing up on\nhttps://github.com/open-telemetry/otel-arrow/pull/3608#discussion_r3670070520\n\n# Changes\n\n* Refactor `convert_to_regex` on `Value` to use `ValueRegex` (mirrors\n`convert_to_string`)\n\n---------\n\nCo-authored-by: albertlockett <a.lockett@f5.com>",
          "timestamp": "2026-08-13T16:36:46Z",
          "tree_id": "373f054ad88e0dfe88ee86b3825a5dc39e5387c8",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/251ce3f2cb0a5116798723981474505f32271e4d"
        },
        "date": 1786650681393,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.41,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.77,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 68.88,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.61,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.41,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lalit_fin@yahoo.com",
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4eb82c9408649b996a5049442495b49195efe0c8",
          "message": "feat(engine): add NUMA-aware core placement planning (#3471)\n\n# Change Summary\n\n**_Note:_** The implementation is based on the design proposal #3317,\nand so subject to the design approval.\n\nThis PR moves pipeline core placement into the controller and makes it\ntopology-aware.\n\nToday, `core_count` pipelines each pick their own cores, starting from\nthe lowest. Two pipelines can silently land on the same cores and\ncompete, and nothing in the engine knows which NUMA node a core belongs\nto.\n\nThis change resolves placement explicitly, before any pipeline launches:\n\n- The engine discovers the process-visible CPU set and the CPU-to-NUMA\nmapping.\n- The controller plans `core_count` placement globally, accounting for\nother pipelines' reserved cores.\n- `core_count` pipelines receive exclusive cores instead of silently\noverlapping.\n- Explicit `core_set` is still honored, but now fails if a requested\ncore is hidden by process affinity or cgroup limits.\n- Startup, live rollout, rollback, and full-config reconcile all share\none placement model.\n\nThe default policy is deterministic NUMA packing: it keeps a pipeline on\na single NUMA node when possible, using stable lowest-node then\nlowest-core ordering. When topology is incomplete, it falls back to\ndeterministic visible-core ordering. The policy sits behind a small\nstrategy interface, so balancing or hardware-aware strategies can be\nadded later without touching placement call sites.\n\nThis PR also adds listener-group metadata as groundwork for future\nsocket-placement work. It does **not** bind sockets, enable\n`SO_REUSEPORT`, or attach eBPF selectors - there is no production\nruntime consumer yet.\n\nThe per-record data path is unchanged. Topology discovery and placement\nplanning run only at startup and during live control operations.\n\n## Breaking Behavior Changes\n\nThese configs now fail loudly instead of doing something surprising:\n\n- `core_count` no longer clamps to the available core count. Requesting\nmore cores than are visible is now a validation error.\n- Multiple `core_count` pipelines no longer overlap on the same first\ncores. Each gets an exclusive set, and startup or live update fails when\nthere aren't enough unreserved cores.\n- `core_count: 0` means \"all unreserved visible cores\" and can now fail\nif none remain.\n- Explicit `core_set` fails if any requested core is hidden by process\naffinity or cgroup CPU limits.\n- Full-config reconcile rejects placement transitions that need another\nlive pipeline to vacate cores first. Stage the shrink or delete first,\nthen apply the growth.\n\nExplicit `core_set`-to-`core_set` overlap is still allowed, as\ndeliberate operator intent.\n\n  * Closes #1837\n\nRelated context, not closed by this PR: #2155 (placement abstraction /\nbalancing), #2974 (socket + eBPF placement).\n\n  ## How are these changes tested?\n\n  New and updated unit coverage:\n\n- Linux topology discovery from sysfs, affinity, and cgroup v2 cpuset\nlimits\n- complete, partial, and unknown topology states, including disjoint\naffinity/cgroup visibility\n  - cpulist parse errors, oversized ranges, and duplicate CPU mappings\n  - NUMA-packing placement and deterministic fallback ordering\n  - strategy injection via the placement interface\n  - startup reservation conflicts across pipelines\n  - `core_count: 0` and omitted-count behavior\n  - explicit `core_set` hidden-core rejection\n  - live rollout, rollback, and reconcile placement handling\n  - conservative vacate-before-claim reconcile rejection\n\n  ## Are there any user-facing changes?\n\n  Yes — see **Breaking Behavior Changes** above.\n\n  ### Changelog\n\n  * [x] Added a `.chloggen/*.yaml` entry\n  * [ ] This PR is a `chore` (indicated in title)\n  * [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-13T21:38:52Z",
          "tree_id": "01b93b34e09abfa1cb6cac802d21550db8d12573",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/4eb82c9408649b996a5049442495b49195efe0c8"
        },
        "date": 1786672344646,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.58,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.77,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.96,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.35,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
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
          "id": "4eb82c9408649b996a5049442495b49195efe0c8",
          "message": "feat(engine): add NUMA-aware core placement planning (#3471)\n\n# Change Summary\n\n**_Note:_** The implementation is based on the design proposal #3317,\nand so subject to the design approval.\n\nThis PR moves pipeline core placement into the controller and makes it\ntopology-aware.\n\nToday, `core_count` pipelines each pick their own cores, starting from\nthe lowest. Two pipelines can silently land on the same cores and\ncompete, and nothing in the engine knows which NUMA node a core belongs\nto.\n\nThis change resolves placement explicitly, before any pipeline launches:\n\n- The engine discovers the process-visible CPU set and the CPU-to-NUMA\nmapping.\n- The controller plans `core_count` placement globally, accounting for\nother pipelines' reserved cores.\n- `core_count` pipelines receive exclusive cores instead of silently\noverlapping.\n- Explicit `core_set` is still honored, but now fails if a requested\ncore is hidden by process affinity or cgroup limits.\n- Startup, live rollout, rollback, and full-config reconcile all share\none placement model.\n\nThe default policy is deterministic NUMA packing: it keeps a pipeline on\na single NUMA node when possible, using stable lowest-node then\nlowest-core ordering. When topology is incomplete, it falls back to\ndeterministic visible-core ordering. The policy sits behind a small\nstrategy interface, so balancing or hardware-aware strategies can be\nadded later without touching placement call sites.\n\nThis PR also adds listener-group metadata as groundwork for future\nsocket-placement work. It does **not** bind sockets, enable\n`SO_REUSEPORT`, or attach eBPF selectors - there is no production\nruntime consumer yet.\n\nThe per-record data path is unchanged. Topology discovery and placement\nplanning run only at startup and during live control operations.\n\n## Breaking Behavior Changes\n\nThese configs now fail loudly instead of doing something surprising:\n\n- `core_count` no longer clamps to the available core count. Requesting\nmore cores than are visible is now a validation error.\n- Multiple `core_count` pipelines no longer overlap on the same first\ncores. Each gets an exclusive set, and startup or live update fails when\nthere aren't enough unreserved cores.\n- `core_count: 0` means \"all unreserved visible cores\" and can now fail\nif none remain.\n- Explicit `core_set` fails if any requested core is hidden by process\naffinity or cgroup CPU limits.\n- Full-config reconcile rejects placement transitions that need another\nlive pipeline to vacate cores first. Stage the shrink or delete first,\nthen apply the growth.\n\nExplicit `core_set`-to-`core_set` overlap is still allowed, as\ndeliberate operator intent.\n\n  * Closes #1837\n\nRelated context, not closed by this PR: #2155 (placement abstraction /\nbalancing), #2974 (socket + eBPF placement).\n\n  ## How are these changes tested?\n\n  New and updated unit coverage:\n\n- Linux topology discovery from sysfs, affinity, and cgroup v2 cpuset\nlimits\n- complete, partial, and unknown topology states, including disjoint\naffinity/cgroup visibility\n  - cpulist parse errors, oversized ranges, and duplicate CPU mappings\n  - NUMA-packing placement and deterministic fallback ordering\n  - strategy injection via the placement interface\n  - startup reservation conflicts across pipelines\n  - `core_count: 0` and omitted-count behavior\n  - explicit `core_set` hidden-core rejection\n  - live rollout, rollback, and reconcile placement handling\n  - conservative vacate-before-claim reconcile rejection\n\n  ## Are there any user-facing changes?\n\n  Yes — see **Breaking Behavior Changes** above.\n\n  ### Changelog\n\n  * [x] Added a `.chloggen/*.yaml` entry\n  * [ ] This PR is a `chore` (indicated in title)\n  * [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-13T21:38:52Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/4eb82c9408649b996a5049442495b49195efe0c8"
        },
        "date": 1786674371606,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.58,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.77,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.96,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.35,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "66651184+utpilla@users.noreply.github.com",
            "name": "Utkarsh Umesan Pillai",
            "username": "utpilla"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "690356560898fa361f547f4c8a800457c9f7f11e",
          "message": "Add On-Behalf-Of (OBO) configuration to the Geneva exporter (#3754)\n\n# Change Summary\n\nAdds an optional `obo` configuration block to the Geneva exporter,\nletting a single agent upload telemetry on behalf of multiple customer\nidentities. Previously the exporter hardcoded  `obo_event_map: None`, so\nOBO was unreachable even though the underlying `geneva-uploader` library\nfully supports it.\n\nThe uploader already carries this through to the endpoint as\n`onbehalfid` / `onbehalfannotations` query parameters. The exporter just\nneeded to expose the config and wire it in.\n\nConfig shape:\n\n```yaml\nobo:\n  events:\n    AuditLogs:                       # destination table name (post event_name_mapping)\n      identity: \"Microsoft.AuditService\"\n      annotations: '<Config onBehalfFields=\"resourceId\" />'   # optional\n    RawLogs:\n      identity: \"Microsoft.RawService\"\n```\n\nA single flat `obo.events` map is shared across logs and spans, keyed by\nevent/table name. Events not listed are uploaded without OBO;\nomitting obo  preserves existing behavior.\n\nKey detail: keys are the destination table name\n\nOBO entries key on the event/table name after  `event_name_mapping` \nresolves it, not the pre-mapping source value. The uploader resolves the\ndestination first, then looks up OBO by that name. Keying on the source\nvalue silently disables OBO (non-fatal).\n\n## What issue does this PR close?\n\n## How are these changes tested?\n\n## Are there any user-facing changes?\n\n### Changelog\n\n<!--\nUser-facing changes need a .chloggen/*.yaml entry. Copy the\nTEMPLATE.yaml\nin go/.chloggen/ or rust/otap-dataflow/.chloggen/ and fill in the\nfields.\nIf not required, include `chore` in the PR title.\n-->\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-14T05:53:25Z",
          "tree_id": "9560f4a5b2eb48ff1a3d10d16734ef63ff1d4f38",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/690356560898fa361f547f4c8a800457c9f7f11e"
        },
        "date": 1786693485897,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.58,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.77,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.96,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.35,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.26,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pritishnahar@gmail.com",
            "name": "Pritish Nahar",
            "username": "pritishnahar95"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ae289471953be1e885fb1c4425d01d59f5370491",
          "message": "feat(otlp_grpc_exporter): consume the bearer_token_provider capability (#3717)\n\nThe OTLP gRPC exporter can now authenticate with a refreshed OAuth\nbearer token by binding the `bearer_token_provider` capability. Binding\nis optional and additive; without it behavior is unchanged.\n\nThe token is stamped as `authorization: Bearer <token>` per request and\ntakes precedence over statically configured and propagated headers.\nWhile no usable token is cached the exporter back-pressures upstream\ninstead of sending unauthenticated requests, and with a provider bound\nan `UNAUTHENTICATED` response invalidates exactly the rejected token\ngeneration and is retried.\n\nMove `BearerAuth` from the OTLP HTTP exporter to `exporters/common` so\nboth exporters share it, generic over a `BearerAuthEvents` type so each\nexporter keeps its own `otlp.exporter.{grpc,http}.*` event names.\n\n# Change Summary\n\n<!--Replace with a brief summary of the change in this PR-->\n\n## What issue does this PR close?\n\n<!--We highly recommend correlation of every PR to an issue-->\n\n* Closes #3479 \n\n## How are these changes tested?\n\n- Unit tests\n- Local integration test with OTLP GRPC Exporter and Keycloak on an AKS\ncluster\n- Local integration test with OTLP HTTP Exporter and Keycloak on an AKS\ncluster\n\n## Are there any user-facing changes?\n\n Yes, see exporter README updates\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-14T18:25:24Z",
          "tree_id": "4cdd202dfbcbda3f0068d7652ef22b8d7bbf24c0",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ae289471953be1e885fb1c4425d01d59f5370491"
        },
        "date": 1786737391360,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.58,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.55,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.75,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.9,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.03,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.66,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.27,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.35,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
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
          "id": "ffdb34b098b94b09f0ee4a7b93ca8f2107d240a8",
          "message": "chore(pdata): add view-based OTLP JSON serialization (#3764)\n\n# Change Summary\n\nAdds backend-agnostic OTLP JSON serialization for logs, metrics, and\ntraces using pdata views. Supports owned protobuf, raw protobuf, and\nOTAP Arrow backends without materializing intermediate messages.\n\n## How are these changes tested?\n\n- Added 11 focused serialization and backend-parity tests.\n\n## Are there any user-facing changes?\n\nNo. This adds an internal serialization API without changing component\nbehavior or configuration.\n\n### Changelog\n\n- [ ] Added a .chloggen/*.yaml entry\n- [x] This PR is a chore (indicated in title)\n- [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-14T22:37:49Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ffdb34b098b94b09f0ee4a7b93ca8f2107d240a8"
        },
        "date": 1786758003158,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 81.58,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.55,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.75,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.9,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.03,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.66,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.27,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.35,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.25,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 100.6,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "l.querel@f5.com",
            "name": "Laurent Quérel",
            "username": "lquerel"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ffdb34b098b94b09f0ee4a7b93ca8f2107d240a8",
          "message": "chore(pdata): add view-based OTLP JSON serialization (#3764)\n\n# Change Summary\n\nAdds backend-agnostic OTLP JSON serialization for logs, metrics, and\ntraces using pdata views. Supports owned protobuf, raw protobuf, and\nOTAP Arrow backends without materializing intermediate messages.\n\n## How are these changes tested?\n\n- Added 11 focused serialization and backend-parity tests.\n\n## Are there any user-facing changes?\n\nNo. This adds an internal serialization API without changing component\nbehavior or configuration.\n\n### Changelog\n\n- [ ] Added a .chloggen/*.yaml entry\n- [x] This PR is a chore (indicated in title)\n- [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-14T22:37:49Z",
          "tree_id": "6745230180e554926ef89e0586153d827fea3259",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ffdb34b098b94b09f0ee4a7b93ca8f2107d240a8"
        },
        "date": 1786759483465,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.12,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.83,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.29,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "129437996+c1ly@users.noreply.github.com",
            "name": "c1ly",
            "username": "c1ly"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5abeaf8efbd37b6c8519fc8edcd519281e988bbd",
          "message": "[otap-dataflow] Kafka Exporter enhancement + integration tests (#3683)\n\n# Change Summary\n\n- Updated to immediately send a permanent nack for unretryable errors\nthat occur when attempting to send data\n- Add live reconfiguration support\n- Updated config to allow control of dynamic topics via allowlist and\nregex\n- Updated config to allow user to directly update config setting for\nautomatic topic creation\n- Added and reorganized test under various scenario types\n  - Security\n  - Shutdown & Live Reconfiguration\n  - Retry expectations\n  - Delivery semantics\n  - Kafka Integration\n  - Telemetry/Observability\n  - Configuration\n  \n## What issue does this PR close?\n\n* Completes part of #3509 \n\n## How are these changes tested?\n\nintegration tests and unit tests\n\n## Are there any user-facing changes?\n\nyes, allow_auto_create_topics is now exposed directly in the Kafka\nExporter config and overwrites any changes set in the producer_config\nhash map (if configured)\n\n### Changelog\n\n* [ x ] Added a `.chloggen/*.yaml` entry\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-08-15T01:02:30Z",
          "tree_id": "c3d59ff33424a5b6948343f9104ac5168f23ed99",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5abeaf8efbd37b6c8519fc8edcd519281e988bbd"
        },
        "date": 1786780146294,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.12,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.83,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.29,
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
          "id": "5abeaf8efbd37b6c8519fc8edcd519281e988bbd",
          "message": "[otap-dataflow] Kafka Exporter enhancement + integration tests (#3683)\n\n# Change Summary\n\n- Updated to immediately send a permanent nack for unretryable errors\nthat occur when attempting to send data\n- Add live reconfiguration support\n- Updated config to allow control of dynamic topics via allowlist and\nregex\n- Updated config to allow user to directly update config setting for\nautomatic topic creation\n- Added and reorganized test under various scenario types\n  - Security\n  - Shutdown & Live Reconfiguration\n  - Retry expectations\n  - Delivery semantics\n  - Kafka Integration\n  - Telemetry/Observability\n  - Configuration\n  \n## What issue does this PR close?\n\n* Completes part of #3509 \n\n## How are these changes tested?\n\nintegration tests and unit tests\n\n## Are there any user-facing changes?\n\nyes, allow_auto_create_topics is now exposed directly in the Kafka\nExporter config and overwrites any changes set in the producer_config\nhash map (if configured)\n\n### Changelog\n\n* [ x ] Added a `.chloggen/*.yaml` entry\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-08-15T01:02:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5abeaf8efbd37b6c8519fc8edcd519281e988bbd"
        },
        "date": 1786844779558,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.12,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.83,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.29,
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
          "id": "5abeaf8efbd37b6c8519fc8edcd519281e988bbd",
          "message": "[otap-dataflow] Kafka Exporter enhancement + integration tests (#3683)\n\n# Change Summary\n\n- Updated to immediately send a permanent nack for unretryable errors\nthat occur when attempting to send data\n- Add live reconfiguration support\n- Updated config to allow control of dynamic topics via allowlist and\nregex\n- Updated config to allow user to directly update config setting for\nautomatic topic creation\n- Added and reorganized test under various scenario types\n  - Security\n  - Shutdown & Live Reconfiguration\n  - Retry expectations\n  - Delivery semantics\n  - Kafka Integration\n  - Telemetry/Observability\n  - Configuration\n  \n## What issue does this PR close?\n\n* Completes part of #3509 \n\n## How are these changes tested?\n\nintegration tests and unit tests\n\n## Are there any user-facing changes?\n\nyes, allow_auto_create_topics is now exposed directly in the Kafka\nExporter config and overwrites any changes set in the producer_config\nhash map (if configured)\n\n### Changelog\n\n* [ x ] Added a `.chloggen/*.yaml` entry\n\n---------\n\nCo-authored-by: Laurent Quérel <l.querel@f5.com>",
          "timestamp": "2026-08-15T01:02:30Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5abeaf8efbd37b6c8519fc8edcd519281e988bbd"
        },
        "date": 1786930938645,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.12,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.83,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.29,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "29139614+renovate[bot]@users.noreply.github.com",
            "name": "renovate[bot]",
            "username": "renovate[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "57058ad9b922bd8c170c04c3ab6dd0258000c40f",
          "message": "chore(deps): update geneva-uploader digest to 7819998 (#3779)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| geneva-uploader | workspace.dependencies | digest | `f101f2a` →\n`7819998` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on Monday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0NC4yOS41IiwidXBkYXRlZEluVmVyIjoiNDQuMjkuNSIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-08-17T06:16:31Z",
          "tree_id": "bd9e66b8dd6e24b521cd905b3ad089bd4c989650",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/57058ad9b922bd8c170c04c3ab6dd0258000c40f"
        },
        "date": 1786953894724,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.12,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.54,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.83,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.31,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 113.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.29,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "29139614+renovate[bot]@users.noreply.github.com",
            "name": "renovate[bot]",
            "username": "renovate[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a63e93a2d5023369f0e979fa670dd9b2d22f6ae4",
          "message": "chore(deps): update weaver crates to a3db0b7 (#3781)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| weaver_common | workspace.dependencies | digest | `f0b3b36` →\n`a3db0b7` |\n| weaver_forge | workspace.dependencies | digest | `f0b3b36` → `a3db0b7`\n|\n| weaver_resolved_schema | workspace.dependencies | digest | `f0b3b36` →\n`a3db0b7` |\n| weaver_resolver | workspace.dependencies | digest | `f0b3b36` →\n`a3db0b7` |\n| weaver_semconv | workspace.dependencies | digest | `f0b3b36` →\n`a3db0b7` |\n\n---\n\n### Configuration\n\n📅 **Schedule**: (UTC)\n\n- Branch creation\n  - \"before 8am on Monday\"\n- Automerge\n  - At any time (no schedule defined)\n\n🚦 **Automerge**: Disabled by config. Please merge this manually once you\nare satisfied.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about these\nupdates again.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend Renovate](https://mend.io/renovate/).\nView the [repository job\nlog](https://developer.mend.io/github/open-telemetry/otel-arrow).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0NC4yOS41IiwidXBkYXRlZEluVmVyIjoiNDQuMjkuNSIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIl19-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2026-08-17T17:04:24Z",
          "tree_id": "19eb0d0f87d4f3d792c355744b727fbe190806fa",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a63e93a2d5023369f0e979fa670dd9b2d22f6ae4"
        },
        "date": 1786995972455,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.26,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.55,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.88,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.65,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.3,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.09,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.41,
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
          "id": "ded78bb19e7bdc01c5426752437481cbbadcbd87",
          "message": "Align Kafka internal telemetry with enum-based metric attributes (#3760)\n\n# Change Summary\n\nAlign the Kafka receiver and exporter internal telemetry with the\nenum-based\nattribute and metric-set patterns used by the OTLP, OTAP, ...\n\nThis change:\n- Introduces bounded attributes for signal, outcome, error type,\nrejection reason, operation, and topic source.\n- Adds actionable receiver telemetry for message lifecycle,\nacknowledgements, rejections, offset commits, transport errors, consumer\nlag, inflight records, duplicate detection, and consumer-group\nrebalances.\n- Reuses the common `exporter.pdata.exports` metrics for Kafka export\noutcomes.\n- Adds exporter metrics for end-to-end latency, encoding and delivery\nlatency, payload size, failure classification, and routing source.\n- Avoids using Kafka topic names as metric attributes, keeping\ncardinality bounded for regex subscriptions and dynamic tenant routing.\n- Documents the new metric contracts and migration from the legacy\nmetrics.\n\n## What issue does this PR close?\n\n* Related to #3300 and #3530 \n\n## How are these changes tested?\n\n- cargo xtask check\n\n## Are there any user-facing changes?\n\nYes. The Kafka receiver and exporter internal metric names and\ndimensions have\nchanged. Operators must migrate existing dashboards, alerts, and queries\nto the\ndocumented metric sets and bounded attributes. Kafka exporter duration\nmetrics\nare now reported in seconds.\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-17T23:55:15Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ded78bb19e7bdc01c5426752437481cbbadcbd87"
        },
        "date": 1787019073358,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.26,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.55,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.88,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.65,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.3,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.09,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.41,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "l.querel@f5.com",
            "name": "Laurent Quérel",
            "username": "lquerel"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ded78bb19e7bdc01c5426752437481cbbadcbd87",
          "message": "Align Kafka internal telemetry with enum-based metric attributes (#3760)\n\n# Change Summary\n\nAlign the Kafka receiver and exporter internal telemetry with the\nenum-based\nattribute and metric-set patterns used by the OTLP, OTAP, ...\n\nThis change:\n- Introduces bounded attributes for signal, outcome, error type,\nrejection reason, operation, and topic source.\n- Adds actionable receiver telemetry for message lifecycle,\nacknowledgements, rejections, offset commits, transport errors, consumer\nlag, inflight records, duplicate detection, and consumer-group\nrebalances.\n- Reuses the common `exporter.pdata.exports` metrics for Kafka export\noutcomes.\n- Adds exporter metrics for end-to-end latency, encoding and delivery\nlatency, payload size, failure classification, and routing source.\n- Avoids using Kafka topic names as metric attributes, keeping\ncardinality bounded for regex subscriptions and dynamic tenant routing.\n- Documents the new metric contracts and migration from the legacy\nmetrics.\n\n## What issue does this PR close?\n\n* Related to #3300 and #3530 \n\n## How are these changes tested?\n\n- cargo xtask check\n\n## Are there any user-facing changes?\n\nYes. The Kafka receiver and exporter internal metric names and\ndimensions have\nchanged. Operators must migrate existing dashboards, alerts, and queries\nto the\ndocumented metric sets and bounded attributes. Kafka exporter duration\nmetrics\nare now reported in seconds.\n\n### Changelog\n\n* [x] Added a `.chloggen/*.yaml` entry\n* [ ] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-17T23:55:15Z",
          "tree_id": "c30ba729c8313dee5779f694d95a9b1de33a2531",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ded78bb19e7bdc01c5426752437481cbbadcbd87"
        },
        "date": 1787025026549,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.33,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.59,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.82,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.71,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.28,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.54,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "l.querel@f5.com",
            "name": "Laurent Quérel",
            "username": "lquerel"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7c4fb9a42b7f858d6e97bf08d8ffb2889a04f8e0",
          "message": "Otap enum attrs (#3534)\n\n**Not ready for review at all!**\n\n# Change Summary\n\nRefactor and align the OTAP receiver and exporter internal telemetry\nusing bounded, enum-based attributes, following the approach established\nfor OTLP telemetry.\n\nThis change:\n\n- Reuses the common signal, outcome, and rejection error-type enums.\n- Aligns OTAP and OTLP acknowledgement and rejection dimensions.\n- Adds receiver batch lifecycle metrics for started and completed\nbatches and payload size, partitioned by signal.\n- Distinguishes stream-level and batch-level receiver rejections by\nbounded `error.type`.\n- Partitions exporter outcome and duration metrics by signal and\nterminal outcome.\n- Adds per-signal OTAP stream queue, encoding, correlation, and response\nmetrics.\n- Replaces lossy per-observation exporter timing messages with\nfixed-memory, per-worker aggregation.\n- Flushes worker timing aggregates during telemetry collection and\nshutdown without dropping or duplicating observations.\n- Renames OTAP metric sets to follow the `receiver.otap.*` and\n`exporter.otap.*` conventions.\n- Updates the component telemetry documentation.\n\n## What issue does this PR close?\n\n* Closes #3300\n\n## How are these changes tested?\n\n- Added tests verifying metric isolation across signal, outcome,\nrejection scope, and error type.\n- Added tests verifying enum attributes are preserved in terminal\nsnapshots and exported exactly once.\n- Added a test proving exporter timing aggregation retains more\nobservations than the former bounded update channel could hold and does\nnot duplicate them across collection intervals.\n- Added real gRPC receiver telemetry coverage for:\n  - Fire-and-forget delivery.\n  - Successful ACK routing.\n  - Refused NACK routing.\n  - Invalid batch rejection.\n  - Memory-pressure stream rejection.\n  - Concurrency-limit batch rejection.\n  - Pipeline send failure lifecycle completion.\n- Added exporter integration coverage verifying pdata counts and\nduration histograms for successful and failed OTAP responses.\n- Ran `cargo xtask check`\n\n## Are there any user-facing changes?\n\nYes. OTAP internal metric names and attribute sets have changed. Metrics\nnow use bounded `signal`, `outcome`, and `error.type` dimensions and\nconsistently follow the `receiver.otap.*` and `exporter.otap.*` naming\nconventions.\n\nExporter stream timing telemetry is also collected reliably under load\nusing fixed-memory aggregation instead of potentially dropping\nobservations when an internal channel is full.\n\n### Changelog\n\n- [x] Added a `.chloggen/*.yaml` entry\n- [ ] This PR is a `chore` (indicated in title)\n- [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-18T05:32:24Z",
          "tree_id": "4d0cd708d1ed32ecdf10d4039f088198d10feb8a",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/7c4fb9a42b7f858d6e97bf08d8ffb2889a04f8e0"
        },
        "date": 1787042643019,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.6,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 4.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.94,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.71,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.55,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "cijo.thomas@gmail.com",
            "name": "Cijo Thomas",
            "username": "cijothomas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "645c40977fe58742128b17c7d6b161804dc761a4",
          "message": "chore(deps): update rust crate h2 to 0.4.16 [security] (#3804)\n\n`cargo deny` fails on every PR and on `main` since RUSTSEC-2026-0258 was\npublished: `h2` accepted and queued empty DATA frames without limit,\nwhich can lead to unbounded memory usage or a panic on length overflow\nwhen streams are not drained. Patched in h2 0.4.16.\n\nThis bumps only the `h2` entry in `rust/otap-dataflow/Cargo.lock` (2\nlines). Running `cargo update -p h2` locally also re-resolved a number\nof unrelated `windows-sys` edges, so the lock entry was updated directly\nto keep the diff minimal.\n\nVerified locally: `cargo deny check advisories` reports `advisories ok`,\nand `cargo check --workspace --locked` succeeds.\n\n### Changelog\n\n* [ ] Added a `.chloggen/*.yaml` entry\n* [x] This PR is a `chore` (indicated in title)\n* [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-18T17:25:22Z",
          "tree_id": "ce27a9b37968609613f642a3c71d386260cccb9d",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/645c40977fe58742128b17c7d6b161804dc761a4"
        },
        "date": 1787084738735,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.64,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.61,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 4.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.96,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.85,
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
          "id": "2e9fff0f0b8808f44b663fdf27a09b2034a5096b",
          "message": "  feat(geneva-exporter): support account group routing (#3799)\n\n## Summary\n\n- add required `account_routing.default_group` configuration to the\nGeneva exporter\n- support optional destination event/table overrides through\n`account_routing.events`\n  - pass account routing into `geneva-uploader`\n- preserve the complete logical-group-to-primary-moniker map from\nagent-fed credentials\n- update `geneva-uploader` to the revision containing multi-moniker\nrouting\n  - update Geneva examples, documentation, tests, and changelog\n\n  This integrates the account-group routing added by\n  https://github.com/open-telemetry/opentelemetry-rust-contrib/pull/747.\n\n  ## Routing model\n\n  YAML config selects logical GCS account groups:\n\n  ```yaml\n  account_routing:\n    default_group: \"diagnostics\"\n    events:\n      AuditLogs: \"audit\"\n      SecurityEvents: \"security\"\n```\n\n  The events keys are final destination event/table names after\n  event_name_mapping has run. Events without an exact override use\n  default_group.\n\n  Physical monikers are not configured in YAML. The uploader resolves the chosen\n  logical group against the current primary-moniker mapping supplied by GCS or an\n  agent-fed credential snapshot.\n\n  ## Breaking configuration change\n\n  Every Geneva exporter configuration must now provide:\n\n```yaml\n  account_routing:\n    default_group: \"<logical-account-group>\"\n```\n\n  Existing configurations must add the logical GCS account group that should\n  receive events without an explicit override.\n\n  ## Validation\n\n  - cargo xtask check\n  - cargo test -p otap-df-contrib-nodes --features geneva-exporter geneva_exporter\n      - 87 passed\n\n  - python3 tools/sanitycheck.py\n  - make chlog-validate\n  - Markdown lint\n  - git diff --check\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-19T00:30:08Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2e9fff0f0b8808f44b663fdf27a09b2034a5096b"
        },
        "date": 1787103685896,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.64,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.61,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 4.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.96,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lalit_fin@yahoo.com",
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2e9fff0f0b8808f44b663fdf27a09b2034a5096b",
          "message": "  feat(geneva-exporter): support account group routing (#3799)\n\n## Summary\n\n- add required `account_routing.default_group` configuration to the\nGeneva exporter\n- support optional destination event/table overrides through\n`account_routing.events`\n  - pass account routing into `geneva-uploader`\n- preserve the complete logical-group-to-primary-moniker map from\nagent-fed credentials\n- update `geneva-uploader` to the revision containing multi-moniker\nrouting\n  - update Geneva examples, documentation, tests, and changelog\n\n  This integrates the account-group routing added by\n  https://github.com/open-telemetry/opentelemetry-rust-contrib/pull/747.\n\n  ## Routing model\n\n  YAML config selects logical GCS account groups:\n\n  ```yaml\n  account_routing:\n    default_group: \"diagnostics\"\n    events:\n      AuditLogs: \"audit\"\n      SecurityEvents: \"security\"\n```\n\n  The events keys are final destination event/table names after\n  event_name_mapping has run. Events without an exact override use\n  default_group.\n\n  Physical monikers are not configured in YAML. The uploader resolves the chosen\n  logical group against the current primary-moniker mapping supplied by GCS or an\n  agent-fed credential snapshot.\n\n  ## Breaking configuration change\n\n  Every Geneva exporter configuration must now provide:\n\n```yaml\n  account_routing:\n    default_group: \"<logical-account-group>\"\n```\n\n  Existing configurations must add the logical GCS account group that should\n  receive events without an explicit override.\n\n  ## Validation\n\n  - cargo xtask check\n  - cargo test -p otap-df-contrib-nodes --features geneva-exporter geneva_exporter\n      - 87 passed\n\n  - python3 tools/sanitycheck.py\n  - make chlog-validate\n  - Markdown lint\n  - git diff --check\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-19T00:30:08Z",
          "tree_id": "f18c83f43095b344acbe8e1826af6ed1906e3e08",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/2e9fff0f0b8808f44b663fdf27a09b2034a5096b"
        },
        "date": 1787105079355,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 4.03,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.98,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.53,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.59,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.85,
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
          "id": "1f0f51b347e76a5fd58028e52db3225b2428787d",
          "message": "chore: [Geneva Exporter] Update the YAML to show events mapping example (#3824)\n\n# Change summary\n- Update the example YAML to add some comments and also show event\nmapping\n\nCo-authored-by: Utkarsh Umesan Pillai <utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-19T23:12:06Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1f0f51b347e76a5fd58028e52db3225b2428787d"
        },
        "date": 1787194048031,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 4.03,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.98,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.53,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.59,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "66651184+utpilla@users.noreply.github.com",
            "name": "Utkarsh Umesan Pillai",
            "username": "utpilla"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1f0f51b347e76a5fd58028e52db3225b2428787d",
          "message": "chore: [Geneva Exporter] Update the YAML to show events mapping example (#3824)\n\n# Change summary\n- Update the example YAML to add some comments and also show event\nmapping\n\nCo-authored-by: Utkarsh Umesan Pillai <utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-19T23:12:06Z",
          "tree_id": "78b202c59eed7b4bc588d67457241b261a0fafec",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/1f0f51b347e76a5fd58028e52db3225b2428787d"
        },
        "date": 1787203577237,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.53,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.61,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.92,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.43,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "136855179+ragumarimuthu-git@users.noreply.github.com",
            "name": "Ragu Marimuthu",
            "username": "ragumarimuthu-git"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8b073420594a4f3d5f5fed104c500b2ca5714c4f",
          "message": "fix(filter): suppress empty filtered payloads (#3790)\n\n## Summary\n- acknowledge fully filtered pdata instead of forwarding an empty\npayload downstream\n- prevent empty OTLP HTTP export requests when all items are removed\n- cover suppression and ACK propagation for logs, metrics, and traces\n\nRelated to #3444.\n\n## Testing\n- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo xtask\ncheck`\n- `go test --tags=assert ./...` (two existing Windows failures:\n`TestOtlpMetricsProfiler` temp-file locking and `TestWError` line-number\nexpectations)\n- end-to-end OTLP HTTP probe confirmed 0 requests and 0 logs for a fully\nfiltered batch\n\n---------\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>\nCo-authored-by: Lalit Kumar Bhasin <lalit_fin@yahoo.com>",
          "timestamp": "2026-08-20T05:18:32Z",
          "tree_id": "49b5f4956c8285cb9f09e5a65302267b9aaa6e1e",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/8b073420594a4f3d5f5fed104c500b2ca5714c4f"
        },
        "date": 1787212993647,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.6,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.06,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 2.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.94,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 2.9,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 69.93,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.68,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.05,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.63,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.79,
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
          "id": "26b710aa0c6e5900e94523992c018104db6cfe24",
          "message": "chore(engine): add local retained-work accounting (#3756)\n\n# Change Summary\n\nAdd a runtime-local retained-work account and non-`Send` ownership\nticket.\n\nThe account tracks known retained bytes and unknown-size items. Tickets\nrefund their charge exactly once on explicit completion. Dropping an\nunresolved ticket also refunds the charge and records abandonment.\n\nChecked arithmetic reports overflow and underflow as accounting\ncorruption.\n\nThis PR does not add runtime wiring, attribution, metrics export,\nconfiguration, enforcement, escrow, or production charge sites.\n\n## Background\n\nThe retained-work pilot needs a runtime-local accounting primitive\nbefore scope wiring, metrics, or processor integration can be added.\n\n## What issue does this PR close?\n\n* Part of #3272\n\n## How are these changes tested?\n\n- `cargo check -p otap-df-engine`\n- `cargo test -p otap-df-engine retained_work::tests`\n- `cargo test -p otap-df-engine --doc`\n- `cargo clippy -p otap-df-engine --all-targets -- -D warnings`\n- `cargo xtask check`\n- `python3 tools/sanitycheck.py`\n- `git diff --check`\n\n## Are there any user-facing changes?\n\nNo. This PR adds an internal accounting primitive without changing\nruntime behavior, configuration, or exported telemetry.\n\n  ### Changelog\n\n  * [ ] Added a `.chloggen/*.yaml` entry\n  * [x] This PR is a `chore` (indicated in title)\n  * [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-21T00:33:27Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/26b710aa0c6e5900e94523992c018104db6cfe24"
        },
        "date": 1787294857670,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 3.96,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.15,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.73,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.44,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.85,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lalit_fin@yahoo.com",
            "name": "Lalit Kumar Bhasin",
            "username": "lalitb"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "26b710aa0c6e5900e94523992c018104db6cfe24",
          "message": "chore(engine): add local retained-work accounting (#3756)\n\n# Change Summary\n\nAdd a runtime-local retained-work account and non-`Send` ownership\nticket.\n\nThe account tracks known retained bytes and unknown-size items. Tickets\nrefund their charge exactly once on explicit completion. Dropping an\nunresolved ticket also refunds the charge and records abandonment.\n\nChecked arithmetic reports overflow and underflow as accounting\ncorruption.\n\nThis PR does not add runtime wiring, attribution, metrics export,\nconfiguration, enforcement, escrow, or production charge sites.\n\n## Background\n\nThe retained-work pilot needs a runtime-local accounting primitive\nbefore scope wiring, metrics, or processor integration can be added.\n\n## What issue does this PR close?\n\n* Part of #3272\n\n## How are these changes tested?\n\n- `cargo check -p otap-df-engine`\n- `cargo test -p otap-df-engine retained_work::tests`\n- `cargo test -p otap-df-engine --doc`\n- `cargo clippy -p otap-df-engine --all-targets -- -D warnings`\n- `cargo xtask check`\n- `python3 tools/sanitycheck.py`\n- `git diff --check`\n\n## Are there any user-facing changes?\n\nNo. This PR adds an internal accounting primitive without changing\nruntime behavior, configuration, or exported telemetry.\n\n  ### Changelog\n\n  * [ ] Added a `.chloggen/*.yaml` entry\n  * [x] This PR is a `chore` (indicated in title)\n  * [ ] This is a documentation-only PR.",
          "timestamp": "2026-08-21T00:33:27Z",
          "tree_id": "8cd2ea07ca4174d842b265ce078423c020970e0c",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/26b710aa0c6e5900e94523992c018104db6cfe24"
        },
        "date": 1787306168422,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.74,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "49cf901db37f697e75adfbf380c4fdd8c18c2d67",
          "message": "chore(repo): Rebalance PR size labels based on recent data (#3854)\n\n# Chore Summary\n\nThe current thresholds put 63% of the 197 PRs merged between July 21 and\nAugust 21, 2026 in `size/L` or `size/XL`. Automation is not driving the\nresult: 72% of the 169 human-authored PRs were labeled L or XL.\n\n| Label | Current range | Current PRs | Current share | New range |\nProjected PRs | Projected share |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n| `size/XS` | 0-8 | 30 | 15% | 0-9 | 31 | 16% |\n| `size/S` | 9-28 | 20 | 10% | 10-29 | 19 | 10% |\n| `size/M` | 29-98 | 23 | 12% | 30-249 | 59 | 30% |\n| `size/L` | 99-498 | 57 | 29% | 250-1,249 | 53 | 27% |\n| `size/XL` | 499+ | 67 | 34% | 1,250+ | 35 | 18% |\n\nBoundaries at 250 and 1,250 make M and L representative of a typical\nchange while keeping XL as a warning for roughly the largest fifth of\nPRs.\n\nI am open to discussion of whether or not we should merge this PR versus\ntrying to keep existing limits and encouraging contributors to break up\nwork into multiple pieces.",
          "timestamp": "2026-08-21T18:32:04Z",
          "tree_id": "22c240788136ca98a61775d5a35e1011391758f8",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/49cf901db37f697e75adfbf380c4fdd8c18c2d67"
        },
        "date": 1787369839720,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.74,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.62,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_core_nodes",
            "value": 4,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otap_df_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_core_nodes",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.46,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otap_df_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.45,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.91,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mmaratov@microsoft.com",
            "name": "Maksat Maratov",
            "username": "maksmara"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b6c89a1811bef396c531145227b3130066d61c20",
          "message": "feat(console-exporter): add compact histogram percentile estimates (#3850)\n\n# Change summary\n\nAdd approximate p50, p90, and p99 values to compact pretty output for\nexplicit\nand exponential histograms.\n\nThe estimator:\n\n- uses nearest-rank bucket selection;\n- uses arithmetic midpoints for explicit buckets and geometric midpoints\nfor\n  exponential buckets;\n- handles negative, zero, and positive exponential bucket populations;\n- supports the full OTLP `sint32` scale range;\n- marks estimates with `~=` to distinguish them from exact values;\n- omits estimates when bucket data is empty, internally inconsistent, or\ncannot\n  be represented safely;\n- produces equivalent output for OTLP and OTAP views.\n\nThe PR also corrects raw OTLP ZigZag decoding so exponential histogram\n`scale`\nand bucket `offset` values can use the full `sint32` range.\n\nRaw histogram output remains unchanged.\n\n## Related issue\n\n* Closes #3840\n\n## Validation\n\n- `cargo test -p otap-df-core-nodes console_exporter`\n- `cargo test -p otap-df-pdata decodes_full_sint32_range`\n- `cargo check -p otap-df-core-nodes`\n- `cargo clippy -p otap-df-core-nodes --all-targets -- -D warnings`\n- `cargo clippy -p otap-df-pdata --all-targets -- -D warnings`\n- `npx markdownlint-cli2\nrust/otap-dataflow/crates/core-nodes/src/exporters/console_exporter/README.md`\n- `chloggen validate --config rust/otap-dataflow/.chloggen/config.yaml`\n- `cargo xtask check`\n\n## User-facing changes\n\nCompact console histogram output now includes approximate `p50~=`,\n`p90~=`,\nand `p99~=` fields when sufficient valid bucket data is available.\nIndividual\npercentiles are omitted when they cannot be estimated safely.\n\nRaw OTLP exponential histogram scale and bucket offset values are now\ndecoded\ncorrectly across the full `sint32` range.\n\nRaw histogram output is unchanged.\n\nAdded\n\n`rust/otap-dataflow/.chloggen/console-compact-histogram-percentiles.yaml`.",
          "timestamp": "2026-08-21T23:38:36Z",
          "tree_id": "b56e41aacde9262d9ed3732978b114282beaaeb0",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/b6c89a1811bef396c531145227b3130066d61c20"
        },
        "date": 1787384341479,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.8,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.02,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.22,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.55,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.98,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "OpenTelemetry Bot",
            "username": "opentelemetrybot",
            "email": "107717825+opentelemetrybot@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "929d7e87d6402c4a5a92b97fb0f9d4f5535b983b",
          "message": "chore: use shared OSSF Scorecard workflow (#3861)\n\nDesign discussion: open-telemetry/sig-security#309\n\n## Changes\n\nMigrate OSSF Scorecard to the shared workflow. This limits code scanning\nalerts from Scorecard to `BinaryArtifactsID`, `DangerousWorkflowID`,\n`PinnedDependenciesID`, and `TokenPermissionsID`.",
          "timestamp": "2026-08-22T18:26:47Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/929d7e87d6402c4a5a92b97fb0f9d4f5535b983b"
        },
        "date": 1787451062108,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.8,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.02,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.22,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.55,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.98,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "OpenTelemetry Bot",
            "username": "opentelemetrybot",
            "email": "107717825+opentelemetrybot@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "929d7e87d6402c4a5a92b97fb0f9d4f5535b983b",
          "message": "chore: use shared OSSF Scorecard workflow (#3861)\n\nDesign discussion: open-telemetry/sig-security#309\n\n## Changes\n\nMigrate OSSF Scorecard to the shared workflow. This limits code scanning\nalerts from Scorecard to `BinaryArtifactsID`, `DangerousWorkflowID`,\n`PinnedDependenciesID`, and `TokenPermissionsID`.",
          "timestamp": "2026-08-22T18:26:47Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/929d7e87d6402c4a5a92b97fb0f9d4f5535b983b"
        },
        "date": 1787537113471,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.8,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.63,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.02,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.22,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.7,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.55,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 101.98,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "161134993+Dipanshusinghh@users.noreply.github.com",
            "name": "Dipanshu singh",
            "username": "Dipanshusinghh"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5ea53bc3b3f2ac5e4a76808bf02f3393c9abf92d",
          "message": "Refactor: Migrate syslog_cef_receiver to MeasurementMetricSet (#3748)\n\nMigrates the `syslog_cef_receiver` component to use the new\n`MeasurementMetricSet` with enum attributes instead of flat counters.\n\n### Changes:\n- **`syslog_cef_receiver`**: Replaced the monolithic flat metrics with\nstructured metric buckets (`deliveries`, `rejections`, `transport`,\n`truncations`, and `connections`).\n- **Standard Attributes**: Aligned with the new telemetry patterns by\nutilizing standard `Outcome` and `ReceiverRejectionErrorType` enums,\nalong with a newly introduced `SyslogCefProtocol` (`Tcp`, `Udp`) enum\nfor rich attribution.\n- **Tests & PipelineContext**: Aligned unit tests and metrics\ninitialization with the new telemetry registry expectations.\n- **Changelog & Docs**: Added a `.chloggen` entry and updated\n`README.md` to guide users on the breaking telemetry changes.\n\nPart of the enum-attribute instrumentation migration tracked in #3530.\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <66651184+utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-24T18:32:06Z",
          "tree_id": "ef3256fdf520e9954ba14ef7d7db56cbe34dbfc7",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/5ea53bc3b3f2ac5e4a76808bf02f3393c9abf92d"
        },
        "date": 1787601121075,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.87,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.28,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.73,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.1,
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
          "id": "ebc01cc6fb0c71b9830895fbdaffca89607699a5",
          "message": "feat(engine): Add opt-in `size` metric to node produced/consumed (#3842)\n\n# Change summary\n\nClosing the loop on final piece to match the [Go Collector universal\ntelemetry RFC on auto-instrumented\nmetrics](https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/rfcs/component-universal-telemetry.md#auto-instrumented-metrics)\n🥳\n\nAdds logical payload size to the engine-owned node outcome metrics:\n\n- `node.producer.produced.size{signal,outcome}`\n- `node.consumer.consumed.size{signal,outcome}`\n\nSize follows the existing item-count policy model. `runtime_metrics:\ndetailed` enables both measurements for every node. At `runtime_metrics:\nnormal`, nodes can independently opt in with:\n\n```yaml\npolicies:\n  telemetry:\n    size: true\n```\n\nThe forward path measures the payload once at each produced or consumed\nboundary and stores the value on the context frame. Ack/Nack unwinding\nrecords the stored size with the same signal and outcome as the\ncorresponding message metric.\n\nOTLP payloads report their encoded protobuf length. OTAP payloads report\nlogical Arrow bytes. Cached OTAP sizing avoids repeating the Arrow array\nand buffer walk when the payload is unchanged.\n\nThe `trafficgen-universal-produced-consumed-metrics.yaml` demo includes\nboth item and size policies and prints only produced/consumed node\nmetrics through its internal observability pipeline.\n\n### Sample config run\n\nThe `full` pipeline uses `runtime_metrics: detailed`. Its log sampler\nkeeps one third of log records while metrics and traces pass through\nunchanged:\n\n| Signal | Receiver produced | Sampler consumed | Sampler produced |\nNoop consumed |\n| --- | ---: | ---: | ---: | ---: |\n| Logs messages | 4 | 4 | 4 | 4 |\n| Logs items | 30 | 30 | 10 | 10 |\n| Logs size (By) | 7,472 | 7,472 | 6,448 | 6,448 |\n| Metrics messages | 2 | 2 | 2 | 2 |\n| Metrics items | 18 | 18 | 18 | 18 |\n| Metrics size (By) | 3,710 | 3,710 | 3,710 | 3,710 |\n| Traces messages | 2 | 2 | 2 | 2 |\n| Traces items | 12 | 12 | 12 | 12 |\n| Traces size (By) | 2,290 | 2,290 | 2,290 | 2,290 |\n\nDropping two thirds of the log records reduces logical size from `7,472\nBy` to `6,448 By` rather than by two thirds. OTAP is a columnar,\nrelational representation: resource, scope, attribute, and dictionary\nbuffers remain largely unchanged, while sampling removes primarily the\nper-record offsets and dictionary keys. The synthetic records also\nrepeat values that are stored once in dictionaries. Logical Arrow size\ntherefore describes the resulting representation, not an average record\nsize multiplied by the item count.\n\nThe `partial` pipeline uses `runtime_metrics: normal` and opts only the\nsampler into `item_counts` and `size`:\n\n| Node boundary | Messages (logs / metrics / traces) | Items (logs /\nmetrics / traces) | Size in By (logs / metrics / traces) |\n| --- | ---: | ---: | ---: |\n| Receiver produced | 4 / 2 / 2 | Not present | Not present |\n| Sampler consumed | 4 / 2 / 2 | 30 / 18 / 12 | 7,472 / 3,710 / 2,290 |\n| Sampler produced | 4 / 2 / 2 | 10 / 18 / 12 | 6,448 / 3,710 / 2,290 |\n| Noop consumed | 4 / 2 / 2 | Not present | Not present |\n\n## Related issue\n\n* Closes #2884\n\n## Validation\n\nLocal engine runs\n\n## User-facing changes\n\nYes. Users can enable node-level logical payload size metrics globally\nwith `runtime_metrics: detailed` or per node with\n`policies.telemetry.size: true`.",
          "timestamp": "2026-08-24T22:34:55Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ebc01cc6fb0c71b9830895fbdaffca89607699a5"
        },
        "date": 1787628423913,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.87,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.28,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.73,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.4,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.1,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ebc01cc6fb0c71b9830895fbdaffca89607699a5",
          "message": "feat(engine): Add opt-in `size` metric to node produced/consumed (#3842)\n\n# Change summary\n\nClosing the loop on final piece to match the [Go Collector universal\ntelemetry RFC on auto-instrumented\nmetrics](https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/rfcs/component-universal-telemetry.md#auto-instrumented-metrics)\n🥳\n\nAdds logical payload size to the engine-owned node outcome metrics:\n\n- `node.producer.produced.size{signal,outcome}`\n- `node.consumer.consumed.size{signal,outcome}`\n\nSize follows the existing item-count policy model. `runtime_metrics:\ndetailed` enables both measurements for every node. At `runtime_metrics:\nnormal`, nodes can independently opt in with:\n\n```yaml\npolicies:\n  telemetry:\n    size: true\n```\n\nThe forward path measures the payload once at each produced or consumed\nboundary and stores the value on the context frame. Ack/Nack unwinding\nrecords the stored size with the same signal and outcome as the\ncorresponding message metric.\n\nOTLP payloads report their encoded protobuf length. OTAP payloads report\nlogical Arrow bytes. Cached OTAP sizing avoids repeating the Arrow array\nand buffer walk when the payload is unchanged.\n\nThe `trafficgen-universal-produced-consumed-metrics.yaml` demo includes\nboth item and size policies and prints only produced/consumed node\nmetrics through its internal observability pipeline.\n\n### Sample config run\n\nThe `full` pipeline uses `runtime_metrics: detailed`. Its log sampler\nkeeps one third of log records while metrics and traces pass through\nunchanged:\n\n| Signal | Receiver produced | Sampler consumed | Sampler produced |\nNoop consumed |\n| --- | ---: | ---: | ---: | ---: |\n| Logs messages | 4 | 4 | 4 | 4 |\n| Logs items | 30 | 30 | 10 | 10 |\n| Logs size (By) | 7,472 | 7,472 | 6,448 | 6,448 |\n| Metrics messages | 2 | 2 | 2 | 2 |\n| Metrics items | 18 | 18 | 18 | 18 |\n| Metrics size (By) | 3,710 | 3,710 | 3,710 | 3,710 |\n| Traces messages | 2 | 2 | 2 | 2 |\n| Traces items | 12 | 12 | 12 | 12 |\n| Traces size (By) | 2,290 | 2,290 | 2,290 | 2,290 |\n\nDropping two thirds of the log records reduces logical size from `7,472\nBy` to `6,448 By` rather than by two thirds. OTAP is a columnar,\nrelational representation: resource, scope, attribute, and dictionary\nbuffers remain largely unchanged, while sampling removes primarily the\nper-record offsets and dictionary keys. The synthetic records also\nrepeat values that are stored once in dictionaries. Logical Arrow size\ntherefore describes the resulting representation, not an average record\nsize multiplied by the item count.\n\nThe `partial` pipeline uses `runtime_metrics: normal` and opts only the\nsampler into `item_counts` and `size`:\n\n| Node boundary | Messages (logs / metrics / traces) | Items (logs /\nmetrics / traces) | Size in By (logs / metrics / traces) |\n| --- | ---: | ---: | ---: |\n| Receiver produced | 4 / 2 / 2 | Not present | Not present |\n| Sampler consumed | 4 / 2 / 2 | 30 / 18 / 12 | 7,472 / 3,710 / 2,290 |\n| Sampler produced | 4 / 2 / 2 | 10 / 18 / 12 | 6,448 / 3,710 / 2,290 |\n| Noop consumed | 4 / 2 / 2 | Not present | Not present |\n\n## Related issue\n\n* Closes #2884\n\n## Validation\n\nLocal engine runs\n\n## User-facing changes\n\nYes. Users can enable node-level logical payload size metrics globally\nwith `runtime_metrics: detailed` or per node with\n`policies.telemetry.size: true`.",
          "timestamp": "2026-08-24T22:34:55Z",
          "tree_id": "6f1dbeac218cc717d1e3b3486c9102a47d509598",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/ebc01cc6fb0c71b9830895fbdaffca89607699a5"
        },
        "date": 1787645434474,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.87,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.3,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.73,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.04,
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
          "id": "a942658eab44e66650313ecdce8ca90a924feae7",
          "message": "fix(metrics): Simplify engine-owned node and flow metric names (#3853)\n\n# Change summary\n\nSimplify engine-owned node and flow metric names around the direction of\ndata through the pipeline:\n\n- `node.consumer.consumed.*` becomes `node.input.*`\n- `node.producer.produced.*` becomes `node.output.*`\n- `flow.consumed.*` becomes `flow.input.*`\n- `flow.produced.*` becomes `flow.output.*`\n\nAlso:\n\n- Add `messages` and logical payload `size` measurements to flow input\nand output metrics alongside `items`.\n- Report `flow.compute.duration` as a seconds-based exponential\nhistogram while retaining its processor-compute semantics.\n- Gate flow measurements with a compact interest bitmap so disabled item\nand size metrics do not inspect PData.\n- Consolidate the node-versus-flow example as\n`trafficgen-input-output-metrics.yaml`.\n- Align Rust flow metric types, fields, methods, tests, documentation,\nand configuration values with input/output terminology.\n\n## Related issue\n\n- Related to #3300\n\n### Validation\n\nThe `trafficgen-input-output-metrics.yaml` example produced the\nfollowing representative one-second delta interval. All node rows have\n`outcome=success`.\n\nThe `full` pipeline enables detailed node metrics and a one-processor\nflow around `sampler`:\n\n```text\nreceiver output\n      │\n      ├── sampler node input == flow input\n      │            30 logs in\n      │            20 logs dropped\n      │            10 logs out\n      └── sampler node output == flow output == noop node input\n```\n\nNode and flow metric sets are stacked in pipeline order. Every value\ncell is `traces / metrics / logs`.\n\n| Pipeline boundary | Metric scope | `messages` | `items` | `size` (By)\n|\n| --- | --- | ---: | ---: | ---: |\n| Receiver output | `node.output` | `1 / 2 / 4` | `6 / 18 / 30` | `1145\n/ 3710 / 7472` |\n| Sampler input | `node.input` | `1 / 2 / 4` | `6 / 18 / 30` | `1145 /\n3710 / 7472` |\n| Flow input | `flow.input` | `1 / 2 / 4` | `6 / 18 / 30` | `1145 / 3710\n/ 7472` |\n| Flow output | `flow.output` | `1 / 2 / 4` | `6 / 18 / 10` | `1145 /\n3710 / 6448` |\n| Sampler output | `node.output` | `1 / 2 / 4` | `6 / 18 / 10` | `1145 /\n3710 / 6448` |\n| Flow decision | `flow.dropped` | — | `— / — / 20` | — |\n| Noop input | `node.input` | `1 / 2 / 4` | `6 / 18 / 10` | `1145 / 3710\n/ 6448` |\n\nThe stacked rows show both boundary agreements and the sampler\ntransformation: `30 log items input - 20 dropped = 10 output`.\n\nDuration histograms use seconds. Each cell is `count / sum / min / max`.\n\n| Measurement | Traces | Metrics | Logs |\n| --- | ---: | ---: | ---: |\n| Receiver `node.output.duration` | `1 / 0.0002026 / 0.0002026 /\n0.0002026` | `2 / 0.0003946 / 0.0001809 / 0.0002137` | `4 / 0.0064401 /\n0.0009315 / 0.0022869` |\n| Sampler `node.input.duration` | `1 / 0.0001319 / 0.0001319 /\n0.0001319` | `2 / 0.0002355 / 0.0000921 / 0.0001434` | `4 / 0.005842 /\n0.0008909 / 0.0020428` |\n| Sampler `flow.compute.duration` | `1 / 0.000009 / 0.000009 / 0.000009`\n| `2 / 0.0000223 / 0.0000109 / 0.0000114` | `4 / 0.0053347 / 0.0008101 /\n0.0018651` |\n| Noop `node.input.duration` | `1 / 0.0000063 / 0.0000063 / 0.0000063` |\n`2 / 0.0000189 / 0.000009 / 0.0000099` | `4 / 0.0000316 / 0.0000062 /\n0.000009` |\n\nOver the course of implementation, I was confused about what\n`node.*.duration` actually represented, opened follow-up issue to\nimprove:\n- https://github.com/open-telemetry/otel-arrow/issues/3881\n\nThe `partial` pipeline uses normal runtime metrics and opts only the\nsampler into item and size measurements. Each signal cell remains\n`messages / items / size in bytes`; `—` means that instrument is\ndisabled.\n\n| Boundary | Scope | Traces | Metrics | Logs |\n| --- | --- | ---: | ---: | ---: |\n| Receiver output | `node.output` | `1 / — / —` | `2 / — / —` | `4 / — /\n—` |\n| Sampler input | `node.input` | `1 / 6 / 1145` | `2 / 18 / 3710` | `4 /\n30 / 7472` |\n| Sampler output | `node.output` | `1 / 6 / 1145` | `2 / 18 / 3710` | `4\n/ 10 / 6448` |\n| Noop input | `node.input` | `1 / — / —` | `2 / — / —` | `4 / — / —` |\n\nThe partial pipeline emits no flow scopes or node duration histograms.\n\nThe `no_output` pipeline uses a deterministic filter that drops every\ngenerated log and ACKs without sending. A representative snapshot\ncontained 4 input messages with 40 items and 9792 logical bytes:\n\n| Pipeline boundary | Metric scope | `messages` | `items` | `size` (By)\n|\n| --- | --- | ---: | ---: | ---: |\n| Receiver output | `node.output` | `4` | `40` | `9792` |\n| Filter input | `node.input` | `4` | `40` | `9792` |\n| Flow input | `flow.input` | `4` | `40` | `9792` |\n| Flow decision | `flow.dropped` | — | `40` | — |\n| Flow output | `flow.output` | absent | absent | absent |\n| Filter output | `node.output` | absent | absent | absent |\n| Noop input | `node.input` | absent | absent | absent |\n\n`flow.compute.duration` still records the completed processor work:\n`count=4`, `sum=0.006460706 s`, `min=0.001050001 s`, and\n`max=0.002712202 s`. This demonstrates that an ACK without a send\nfinalizes flow compute and drop accounting without inventing an output\nmessage.\n\n## User-facing changes\n\nReplace:\n\n- `node.consumer.consumed.*` with `node.input.*`\n- `node.producer.produced.*` with `node.output.*`\n- `flow.consumed.*` with `flow.input.*`\n- `flow.produced.*` with `flow.output.*`\n- Flow metric configuration values based on consumed/produced\nterminology with `input_messages`, `input_items`, `input_size`,\n`output_messages`, `output_items`, and `output_size`.\n- Scope selectors targeting the previous names with the corresponding\ninput/output scopes.\n\nTreat `flow.compute.duration` values as seconds and as histogram\nobservations.",
          "timestamp": "2026-08-25T22:23:10Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a942658eab44e66650313ecdce8ca90a924feae7"
        },
        "date": 1787708715490,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.87,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.3,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.73,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.64,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.04,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a942658eab44e66650313ecdce8ca90a924feae7",
          "message": "fix(metrics): Simplify engine-owned node and flow metric names (#3853)\n\n# Change summary\n\nSimplify engine-owned node and flow metric names around the direction of\ndata through the pipeline:\n\n- `node.consumer.consumed.*` becomes `node.input.*`\n- `node.producer.produced.*` becomes `node.output.*`\n- `flow.consumed.*` becomes `flow.input.*`\n- `flow.produced.*` becomes `flow.output.*`\n\nAlso:\n\n- Add `messages` and logical payload `size` measurements to flow input\nand output metrics alongside `items`.\n- Report `flow.compute.duration` as a seconds-based exponential\nhistogram while retaining its processor-compute semantics.\n- Gate flow measurements with a compact interest bitmap so disabled item\nand size metrics do not inspect PData.\n- Consolidate the node-versus-flow example as\n`trafficgen-input-output-metrics.yaml`.\n- Align Rust flow metric types, fields, methods, tests, documentation,\nand configuration values with input/output terminology.\n\n## Related issue\n\n- Related to #3300\n\n### Validation\n\nThe `trafficgen-input-output-metrics.yaml` example produced the\nfollowing representative one-second delta interval. All node rows have\n`outcome=success`.\n\nThe `full` pipeline enables detailed node metrics and a one-processor\nflow around `sampler`:\n\n```text\nreceiver output\n      │\n      ├── sampler node input == flow input\n      │            30 logs in\n      │            20 logs dropped\n      │            10 logs out\n      └── sampler node output == flow output == noop node input\n```\n\nNode and flow metric sets are stacked in pipeline order. Every value\ncell is `traces / metrics / logs`.\n\n| Pipeline boundary | Metric scope | `messages` | `items` | `size` (By)\n|\n| --- | --- | ---: | ---: | ---: |\n| Receiver output | `node.output` | `1 / 2 / 4` | `6 / 18 / 30` | `1145\n/ 3710 / 7472` |\n| Sampler input | `node.input` | `1 / 2 / 4` | `6 / 18 / 30` | `1145 /\n3710 / 7472` |\n| Flow input | `flow.input` | `1 / 2 / 4` | `6 / 18 / 30` | `1145 / 3710\n/ 7472` |\n| Flow output | `flow.output` | `1 / 2 / 4` | `6 / 18 / 10` | `1145 /\n3710 / 6448` |\n| Sampler output | `node.output` | `1 / 2 / 4` | `6 / 18 / 10` | `1145 /\n3710 / 6448` |\n| Flow decision | `flow.dropped` | — | `— / — / 20` | — |\n| Noop input | `node.input` | `1 / 2 / 4` | `6 / 18 / 10` | `1145 / 3710\n/ 6448` |\n\nThe stacked rows show both boundary agreements and the sampler\ntransformation: `30 log items input - 20 dropped = 10 output`.\n\nDuration histograms use seconds. Each cell is `count / sum / min / max`.\n\n| Measurement | Traces | Metrics | Logs |\n| --- | ---: | ---: | ---: |\n| Receiver `node.output.duration` | `1 / 0.0002026 / 0.0002026 /\n0.0002026` | `2 / 0.0003946 / 0.0001809 / 0.0002137` | `4 / 0.0064401 /\n0.0009315 / 0.0022869` |\n| Sampler `node.input.duration` | `1 / 0.0001319 / 0.0001319 /\n0.0001319` | `2 / 0.0002355 / 0.0000921 / 0.0001434` | `4 / 0.005842 /\n0.0008909 / 0.0020428` |\n| Sampler `flow.compute.duration` | `1 / 0.000009 / 0.000009 / 0.000009`\n| `2 / 0.0000223 / 0.0000109 / 0.0000114` | `4 / 0.0053347 / 0.0008101 /\n0.0018651` |\n| Noop `node.input.duration` | `1 / 0.0000063 / 0.0000063 / 0.0000063` |\n`2 / 0.0000189 / 0.000009 / 0.0000099` | `4 / 0.0000316 / 0.0000062 /\n0.000009` |\n\nOver the course of implementation, I was confused about what\n`node.*.duration` actually represented, opened follow-up issue to\nimprove:\n- https://github.com/open-telemetry/otel-arrow/issues/3881\n\nThe `partial` pipeline uses normal runtime metrics and opts only the\nsampler into item and size measurements. Each signal cell remains\n`messages / items / size in bytes`; `—` means that instrument is\ndisabled.\n\n| Boundary | Scope | Traces | Metrics | Logs |\n| --- | --- | ---: | ---: | ---: |\n| Receiver output | `node.output` | `1 / — / —` | `2 / — / —` | `4 / — /\n—` |\n| Sampler input | `node.input` | `1 / 6 / 1145` | `2 / 18 / 3710` | `4 /\n30 / 7472` |\n| Sampler output | `node.output` | `1 / 6 / 1145` | `2 / 18 / 3710` | `4\n/ 10 / 6448` |\n| Noop input | `node.input` | `1 / — / —` | `2 / — / —` | `4 / — / —` |\n\nThe partial pipeline emits no flow scopes or node duration histograms.\n\nThe `no_output` pipeline uses a deterministic filter that drops every\ngenerated log and ACKs without sending. A representative snapshot\ncontained 4 input messages with 40 items and 9792 logical bytes:\n\n| Pipeline boundary | Metric scope | `messages` | `items` | `size` (By)\n|\n| --- | --- | ---: | ---: | ---: |\n| Receiver output | `node.output` | `4` | `40` | `9792` |\n| Filter input | `node.input` | `4` | `40` | `9792` |\n| Flow input | `flow.input` | `4` | `40` | `9792` |\n| Flow decision | `flow.dropped` | — | `40` | — |\n| Flow output | `flow.output` | absent | absent | absent |\n| Filter output | `node.output` | absent | absent | absent |\n| Noop input | `node.input` | absent | absent | absent |\n\n`flow.compute.duration` still records the completed processor work:\n`count=4`, `sum=0.006460706 s`, `min=0.001050001 s`, and\n`max=0.002712202 s`. This demonstrates that an ACK without a send\nfinalizes flow compute and drop accounting without inventing an output\nmessage.\n\n## User-facing changes\n\nReplace:\n\n- `node.consumer.consumed.*` with `node.input.*`\n- `node.producer.produced.*` with `node.output.*`\n- `flow.consumed.*` with `flow.input.*`\n- `flow.produced.*` with `flow.output.*`\n- Flow metric configuration values based on consumed/produced\nterminology with `input_messages`, `input_items`, `input_size`,\n`output_messages`, `output_items`, and `output_size`.\n- Scope selectors targeting the previous names with the corresponding\ninput/output scopes.\n\nTreat `flow.compute.duration` values as seconds and as histogram\nobservations.",
          "timestamp": "2026-08-25T22:23:10Z",
          "tree_id": "10edc31692041c91d858ef20f0d91df47c92a671",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a942658eab44e66650313ecdce8ca90a924feae7"
        },
        "date": 1787710931163,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.91,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.38,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.55,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.68,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "jmacd@users.noreply.github.com",
            "name": "Joshua MacDonald",
            "username": "jmacd"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c9263bf89e20f35a6f9b2f5cf696fdce68dc729e",
          "message": "chore(toolchain): include rust-analyzer component (#3894)\n\nKeep rust-analyzer available when the repository's pinned Rust toolchain\nis installed or refreshed.\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
          "timestamp": "2026-08-26T22:32:08Z",
          "tree_id": "8208dcaf3e7da1d01cffe74f3d6157279b39a637",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/c9263bf89e20f35a6f9b2f5cf696fdce68dc729e"
        },
        "date": 1787798514012,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.9,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.36,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.74,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.67,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "66651184+utpilla@users.noreply.github.com",
            "name": "Utkarsh Umesan Pillai",
            "username": "utpilla"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a7f8706e54b01c778a8d7bbbf98f92b53aa4bc09",
          "message": "Return client-error status codes for permanent NACKs at the OTLP receivers (#3885)\n\n# Change summary\n\n### What\nWhen the pipeline permanently rejects a request because of the request\nitself (for example, telemetry that fails the Resource Validator\nProcessor with a missing or disallowed resource attribute), the OTLP\nreceivers now return a non-retryable client error (`400 Bad Request` /\ngRPC `INVALID_ARGUMENT`) instead of a retryable `503` / `UNAVAILABLE`.\n\n### Why\nA permanent NACK means \"do not retry; fix your config.\" Previously the\nOTLP/HTTP receiver returned 503 for every NACK, so clients kept\nresending data that could never succeed. gRPC returned `INTERNAL`, which\nis non-retryable but blames the server. Neither could express \"the\nclient sent bad data.\"\n\n## Related issue\n\n* Closes #3826 \n\n## Validation\n\n- Unit tests\n\n## User-facing changes\n\n| Failure | Before | After |\n| --- | --- | --- |\n| Client-caused permanent rejection | `503` / `UNAVAILABLE` (HTTP),\n`INTERNAL` (gRPC) | `400` / `INVALID_ARGUMENT` |\n| Permanent server-side failure | `503` / `INTERNAL` | `500` /\n`INTERNAL` |\n| Transient failure | `503` / `UNAVAILABLE` | `503` / `UNAVAILABLE`\n(unchanged) |\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-27T00:58:24Z",
          "tree_id": "e91aadaf9551ba9f422f3de7fbb762dcb7d2348b",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a7f8706e54b01c778a8d7bbbf98f92b53aa4bc09"
        },
        "date": 1787825508069,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.88,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.32,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.74,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.66,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.1,
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
          "id": "a7f8706e54b01c778a8d7bbbf98f92b53aa4bc09",
          "message": "Return client-error status codes for permanent NACKs at the OTLP receivers (#3885)\n\n# Change summary\n\n### What\nWhen the pipeline permanently rejects a request because of the request\nitself (for example, telemetry that fails the Resource Validator\nProcessor with a missing or disallowed resource attribute), the OTLP\nreceivers now return a non-retryable client error (`400 Bad Request` /\ngRPC `INVALID_ARGUMENT`) instead of a retryable `503` / `UNAVAILABLE`.\n\n### Why\nA permanent NACK means \"do not retry; fix your config.\" Previously the\nOTLP/HTTP receiver returned 503 for every NACK, so clients kept\nresending data that could never succeed. gRPC returned `INTERNAL`, which\nis non-retryable but blames the server. Neither could express \"the\nclient sent bad data.\"\n\n## Related issue\n\n* Closes #3826 \n\n## Validation\n\n- Unit tests\n\n## User-facing changes\n\n| Failure | Before | After |\n| --- | --- | --- |\n| Client-caused permanent rejection | `503` / `UNAVAILABLE` (HTTP),\n`INTERNAL` (gRPC) | `400` / `INVALID_ARGUMENT` |\n| Permanent server-side failure | `503` / `INTERNAL` | `500` /\n`INTERNAL` |\n| Transient failure | `503` / `UNAVAILABLE` | `503` / `UNAVAILABLE`\n(unchanged) |\n\n---------\n\nCo-authored-by: Utkarsh Umesan Pillai <utpilla@users.noreply.github.com>",
          "timestamp": "2026-08-27T00:58:24Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a7f8706e54b01c778a8d7bbbf98f92b53aa4bc09"
        },
        "date": 1787845400429,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.88,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.65,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.32,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.56,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.74,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.66,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.1,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "51d89ded5ad4a169eaa4ba422368e0913f74a738",
          "message": "fix(azure_monitor_exporter): Return permanant NACK for an empty batch (#3903)\n\n# Change summary\n\nMinor follow-up from #3891\n\nTechnically there is no reason to return a retryable NACK from the\nexporter if there is an empty payload detected. While we are fixing this\nspecific issue upstream, it is best for this component to also handle it\ncorrectly.\n\n## Related issue\n\n* Related to #3891\n\n## Validation\n\nUnit test\n\n## User-facing changes\n\nAzure Monitor exporter now permanently rejects empty batches instead of\nallowing them to be retried.",
          "timestamp": "2026-08-27T17:31:15Z",
          "tree_id": "b256841db9bbca727b94a0ae4f97ea6f7aba25c8",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/51d89ded5ad4a169eaa4ba422368e0913f74a738"
        },
        "date": 1787860268133,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 82.89,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.66,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.02,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.35,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.67,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.16,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80d38834e747127ee02559e8c6dafd792193bdb2",
          "message": "chore(repo): Isolate test-only nodes in `dev-nodes` crate to minimize dependency chain (#3912)\n\n# Chore summary\n\nMoves the following nodes into a new `dev-nodes` crate:\n- `traffic_generator_receiver`\n- `delay_processor`\n- `error_exporter`\n- `perf_exporter`\n\nAs #3909 mentions, we do this to unblock publishing of `core-nodes`\ncrate on `crates.io` because there is currently a git Weaver dependency\nwith no published crate. We do not plan to publish `dev-nodes`\nexternally.\n\n## Related issue\n\n- Closes #3909",
          "timestamp": "2026-08-28T01:54:22Z",
          "tree_id": "a454eff494fb050b9ba9bef9f1caa413ecae3fb1",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/80d38834e747127ee02559e8c6dafd792193bdb2"
        },
        "date": 1787903140416,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 83.11,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.93,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.8,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.44,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.99,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.41,
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
          "id": "80d38834e747127ee02559e8c6dafd792193bdb2",
          "message": "chore(repo): Isolate test-only nodes in `dev-nodes` crate to minimize dependency chain (#3912)\n\n# Chore summary\n\nMoves the following nodes into a new `dev-nodes` crate:\n- `traffic_generator_receiver`\n- `delay_processor`\n- `error_exporter`\n- `perf_exporter`\n\nAs #3909 mentions, we do this to unblock publishing of `core-nodes`\ncrate on `crates.io` because there is currently a git Weaver dependency\nwith no published crate. We do not plan to publish `dev-nodes`\nexternally.\n\n## Related issue\n\n- Closes #3909",
          "timestamp": "2026-08-28T01:54:22Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/80d38834e747127ee02559e8c6dafd792193bdb2"
        },
        "date": 1787913044341,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 83.11,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.93,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.97,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.57,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.8,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.44,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.99,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.41,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "684eb91f3bdbc0527b4e5c07f879b6e5944f50eb",
          "message": "chore(release): Setup Wave2 crate publishing (#3905)\n\n# Chore Summary\n\nFollowing successful release of\nhttps://crates.io/crates/otel-arrow-dfe-pdata-views, expand crates.io\npublishing from the `pdata-views` pilot to 14 foundational OTAP Dataflow\ncrates.\n\nThis change:\n\n- adds the package metadata and documentation required to publish the\nselected crates\n- publishes the allowlisted crates in dependency order derived from\nCargo metadata\n- validates same-release dependency versions using Cargo-compatible\nsemver requirements\n- runs a complete preflight before the first irreversible upload\n- verifies existing crate checksums and waits for each published version\nto become available through the Cargo index\n- documents the bootstrap, ownership, trusted-publishing, recovery, and\nemergency-release processes\n- keeps crates with unresolved Git, experimental path, or unpublished\ninternal dependencies outside this publication wave\n\nThe initial release of each new crate still requires a scoped crates.io\ntoken. After bootstrap, releases use trusted publishing through the Push\nRelease workflow.\n\n## Related issue\n\nPart of #1340",
          "timestamp": "2026-08-28T18:36:47Z",
          "tree_id": "44de2f0c469c426e51a0eb733be1bee61656db08",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/684eb91f3bdbc0527b4e5c07f879b6e5944f50eb"
        },
        "date": 1787953082853,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 83.12,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.95,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.6,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.8,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.48,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 114.99,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "drewrelmas@gmail.com",
            "name": "Drew Relmas",
            "username": "drewrelmas"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a3d3101072d2b7e0e0bdbca175ec981854aff015",
          "message": "fix(quiver): Fix semantics of loss metrics and introduce contrasting reclaimed metrics (#3898)\n\n# Change summary\n\n- report durable-buffer retention loss from only the bundles that remain\nunresolved when a segment is removed\n- persist optional logical byte counts per bundle so loss bytes reflect\nthe original OTAP or OTLP payload rather than the containing segment\nfile\n- separate physical storage reclamation into\n`processor.durable_buffer.reclaimed`\n- restore subscriber progress before startup expiry accounting and\nreport deferred file deletion only after removal is confirmed\n\n### Details\n\nQuiver stores multiple bundles in each physical segment. Previously, if\none unresolved bundle kept a segment until retention expiry, the entire\nsegment was reported as lost—including bundles that downstream exporters\nhad already ACKed.\n\n```text\nsegment selected by retention\n├── bundle 0: unresolved  -> logical loss\n└── bundle 1: ACKed       -> excluded from logical loss\n\nphysical segment removal  -> reclaimed storage\n```\n\nRetention now snapshots the union of unresolved bundle indices across\nsubscribers while force-completing the segment under the same\nsubscriber-state lock. Each stored bundle is counted once when any\nsubscriber still needs it, avoiding both ACK races and per-subscriber\ndouble counting.\n\nThe segment manifest now includes an optional nullable `byte_count` for\neach bundle:\n\n- OTAP records use their active-range Arrow logical size\n- OTLP pass-through records use encoded protobuf wire length\n- known zero remains distinct from an unavailable count\n- loss bytes are emitted only when the selected unresolved bundles all\nhave exact counts\n\nThe manifest change is additive. New readers treat a missing\n`byte_count` column as unavailable, while existing readers continue\nlocating known fields by name and ignore the additional column.\n\n## Related issue\n\n* Closes #3892 \n\n## Validation\n\nRegression coverage exercises:\n\n- a partially ACKed segment, verifying only its unresolved bundle\ncontributes bundles, items, and logical bytes to expiry loss\n- a fully ACKed later segment retained behind an incomplete predecessor,\nverifying resolved data is excluded from expiry loss\n- multiple subscribers with overlapping unresolved bundles, verifying\nunion semantics count each stored bundle once\n- startup expiry after subscriber progress restoration, verifying\npersisted ACKs remain excluded\n- immediate, deferred, retried, and abandoned physical deletion,\nverifying reclaimed counters advance only after confirmed removal\n- nullable manifest byte counts, including known zero, known nonzero,\nunavailable counts, and legacy manifests without the new column\n- canonical OTAP active-range and OTLP protobuf-wire byte measurements\n\nA standalone mixed-segment repro sends one log bundle and one metric\nbundle through the same durable buffer, ACKs one downstream, transiently\nNACKs the other, and waits for the segment to expire.\n\n### Single-item batches\n\n| Unresolved bundle | `reclaimed.segments` | `reclaimed.bytes` |\n`loss.bundles` | `loss.bytes` | `loss.items` |\n| --- | ---: | ---: | ---: | ---: | ---: |\n| 1 log | 1 `reason=expired` | 7,344 `reason=expired` | 1\n`reason=expired` | 359 `reason=expired` | 1 `signal=logs,\nreason=expired` |\n| 1 metric datapoint | 1 `reason=expired` | 7,344 `reason=expired` | 1\n`reason=expired` | 329 `reason=expired` | 1 `signal=metrics,\nreason=expired` |\n\n### 100-item batches\n\n| Unresolved bundle | `reclaimed.segments` | `reclaimed.bytes` |\n`loss.bundles` | `loss.bytes` | `loss.items` |\n| --- | ---: | ---: | ---: | ---: | ---: |\n| 100 logs | 1 `reason=expired` | 57,840 `reason=expired` | 1\n`reason=expired` | 24,248 `reason=expired` | 100 `signal=logs,\nreason=expired` |\n| 100 metric datapoints | 1 `reason=expired` | 57,840 `reason=expired` |\n1 `reason=expired` | 26,952 `reason=expired` | 100 `signal=metrics,\nreason=expired` |\n\nFor the 100-item run, the segment contained 51,200 logical payload bytes\nand 6,640 bytes of Arrow IPC and Quiver segment overhead:\n\n```text\n57,840 physical bytes reclaimed\n├── 24,248 logical log bytes\n├── 26,952 logical metric bytes\n└──  6,640 segment encoding overhead\n```\n\nIn every permutation, the physical segment size remained constant while\n`loss.bytes` changed to match only the unresolved bundle. The ACKed\nneighboring bundle was excluded from logical loss.\n\n## User-facing changes\n\n| Previous metric | Replacement | Meaning |\n| --- | --- | --- |\n| `processor.durable_buffer.loss.segments` |\n`processor.durable_buffer.reclaimed.segments` | Physical segment files\nremoved |\n| `processor.durable_buffer.loss.bytes` |\n`processor.durable_buffer.reclaimed.bytes` | Physical persisted bytes\nremoved |\n| N/A | `processor.durable_buffer.loss.bytes` | Logical bytes in\nunresolved bundles |\n| `processor.durable_buffer.loss.bundles` | unchanged | Unresolved\nbundles removed by retention |\n| `processor.durable_buffer.loss.items` | unchanged | Items in\nunresolved bundles, partitioned by signal |\n\nPhysical reclamation advances only after a segment file is confirmed\nremoved. Deferred or abandoned deletion attempts are not reported as\nreclaimed.",
          "timestamp": "2026-08-28T23:29:54Z",
          "tree_id": "cee21b3f61c9afc260ae1620d9e9e7d098680db3",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/a3d3101072d2b7e0e0bdbca175ec981854aff015"
        },
        "date": 1787981761540,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 83.19,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.6,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.8,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 115.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Swapnil Ashtekar",
            "username": "swashtek",
            "email": "46826200+swashtek@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "3065676427079b08049c6167d0d7fd410ff25743",
          "message": "fix(otlp_http_exporter): enhance error handling for HTTP status responses (#3902)\n\n# Change summary\nThe OTLP/HTTP exporter `usesreqwest::Response::error_for_status()` to\ndetect non-2xx responses. This call discards the response body, so any\nexplanation the backend sent back (missing/invalid field, rejection\nreason, rate-limit detail, etc.) was silently lost before it ever\nreached logs or NACK reasons.\n\nI would like to change it to include an explicit status check and add\n`RpcStatus` or falls back to raw `UTF-8` text.\n\n> Chore PR? Open **Preview**, then [use the chore\ntemplate](?template=chore.md).\n\n<!--Replace with a brief summary of the change in this PR-->\n\n## Related issue\nNone\n\n## Validation\nValidated locally while debugging issues like `400 Bad Request`, `503\nServer Unavailable` etc.\n\n## User-facing changes\n\n<!--\nDescribe the impact, or write `None`.\nUser-facing changes require a `.chloggen/*.yaml` entry. If no entry is\nneeded,\ninclude `chore` in the PR title. Documentation-only changes are exempt.\n-->",
          "timestamp": "2026-08-29T06:01:56Z",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/3065676427079b08049c6167d0d7fd410ff25743"
        },
        "date": 1787989221883,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 83.19,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.69,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.99,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3.01,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.6,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.8,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.5,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.16,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 115.06,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.48,
            "unit": "MB"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "46826200+swashtek@users.noreply.github.com",
            "name": "Swapnil Ashtekar",
            "username": "swashtek"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3065676427079b08049c6167d0d7fd410ff25743",
          "message": "fix(otlp_http_exporter): enhance error handling for HTTP status responses (#3902)\n\n# Change summary\nThe OTLP/HTTP exporter `usesreqwest::Response::error_for_status()` to\ndetect non-2xx responses. This call discards the response body, so any\nexplanation the backend sent back (missing/invalid field, rejection\nreason, rate-limit detail, etc.) was silently lost before it ever\nreached logs or NACK reasons.\n\nI would like to change it to include an explicit status check and add\n`RpcStatus` or falls back to raw `UTF-8` text.\n\n> Chore PR? Open **Preview**, then [use the chore\ntemplate](?template=chore.md).\n\n<!--Replace with a brief summary of the change in this PR-->\n\n## Related issue\nNone\n\n## Validation\nValidated locally while debugging issues like `400 Bad Request`, `503\nServer Unavailable` etc.\n\n## User-facing changes\n\n<!--\nDescribe the impact, or write `None`.\nUser-facing changes require a `.chloggen/*.yaml` entry. If no entry is\nneeded,\ninclude `chore` in the PR title. Documentation-only changes are exempt.\n-->",
          "timestamp": "2026-08-29T06:01:56Z",
          "tree_id": "b007f69f2443aad5ef3cc14816dcd55675802530",
          "url": "https://github.com/thompson-tomo/otel-arrow/commit/3065676427079b08049c6167d0d7fd410ff25743"
        },
        "date": 1787991635417,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "linux-amd64-text-size",
            "value": 83.22,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-std",
            "value": 4.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_core_nodes",
            "value": 4.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_array",
            "value": 3.68,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_expr",
            "value": 3.52,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_functions_aggregate",
            "value": 3.04,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_common",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-arrow_cast",
            "value": 3,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-[Unknown]",
            "value": 2.98,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-datafusion_physical_plan",
            "value": 2.92,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-crate-otel_arrow_dfe_query_engine",
            "value": 2.69,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-text-size",
            "value": 70.61,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-std",
            "value": 4.8,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_core_nodes",
            "value": 3.54,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_array",
            "value": 3.51,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_expr",
            "value": 3.17,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_common",
            "value": 2.75,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-arrow_cast",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_physical_plan",
            "value": 2.49,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-datafusion_functions_aggregate",
            "value": 2.47,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-[Unknown]",
            "value": 2.41,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-crate-otel_arrow_dfe_query_engine",
            "value": 2.05,
            "unit": "MB"
          },
          {
            "name": "linux-amd64-binary-size",
            "value": 115.12,
            "unit": "MB"
          },
          {
            "name": "linux-arm64-binary-size",
            "value": 102.48,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}