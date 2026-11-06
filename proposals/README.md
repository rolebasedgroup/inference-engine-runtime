# Proposals

This directory is the home for design proposals that describe significant new features,
API/protocol changes, architectural refactors, and other changes whose impact needs review
before implementation.

It is intentionally inspired by mature enhancement processes used by other projects,
including Kubernetes KEPs, Python PEPs, Rust RFCs, and Spark SIPs. The goal is not to add
heavy process, but to make the motivation, scope, design trade-offs, compatibility impact,
and test strategy explicit and reviewable.

## When to write a proposal

Please create a proposal when a change:

- Introduces or changes a public API, protocol, configuration format, or deployment contract
- Changes the sidecar's architecture, lifecycle, security model, or failure-recovery behavior
- Adds a new inference engine backend or a new cross-component integration
- Has multiple plausible designs and needs an explicit decision
- Could materially affect performance, scalability, observability, or operational behavior

Small fixes, internal refactors, tests, docs, and low-risk additions do not require a proposal.
When in doubt, create a short draft proposal or open an issue first and discuss it.

## Directory layout

```text
proposals/
├── README.md                      # This process guide
├── NNNN-template/
│   ├── README.md                  # Proposal body template
│   └── proposal.yaml              # Machine-readable metadata template
└── NNNN-short-descriptive-title/  # One directory per proposal
    ├── README.md
    └── proposal.yaml
```

## Creating a proposal

1. Pick the next unused four-digit number (`NNNN`). Numbering starts at `0001`.
2. Copy `proposals/NNNN-template` to `proposals/NNNN-short-descriptive-title`.
3. Rename the title in both `README.md` and `proposal.yaml`.
4. Fill in at least:
   - Summary
   - Motivation, Goals, and Non-Goals
   - Proposed solution (may remain high level while under review)
   - Risks and Alternatives
   - Open Questions
5. Submit the proposal in a pull request and request review from relevant maintainers.
6. Merge early and iterate. A proposal can be merged as `draft` while important sections
   remain unresolved; mark those sections with `UNRESOLVED`.

## Lifecycle

A proposal moves through these statuses:

| Status | Meaning |
|--------|---------|
| `draft` | Initial writing and open discussion; content can change freely |
| `review` | Design is sufficiently complete for maintainer review |
| `accepted` | Direction and scope are approved; implementation may begin |
| `implemented` | The design has been delivered and the document reflects the final behavior |
| `rejected` | Reviewed and intentionally not accepted |
| `withdrawn` | Withdrawn by the authors before implementation |
| `superseded` | Replaced by another proposal |
| `deferred` | Valuable but intentionally postponed |

Update `proposal.yaml` whenever status changes. Record major dates and decisions in the
proposal's **Implementation History** section rather than deleting them.

## Writing guidance

- **Lead with the problem, not the implementation.** Explain why the current behavior is
  insufficient before proposing a solution.
- **Keep Goals and Non-Goals concrete.** Non-goals prevent accidental scope creep.
- **Discuss alternatives honestly.** A proposal should show why the recommended design wins
  rather than pretending no other design exists.
- **Make compatibility and rollback explicit.** This project runs as a sidecar in managed
  deployments, so protocol, environment-variable, metric, and engine-version compatibility
  are first-class concerns.
- **Define how the change will be tested and observed.** A feature is not accepted until its
  expected behavior and failure modes are verifiable.
- **Separate facts from assumptions.** Label assumptions and turn them into open questions
  when they could affect the design.
- **Prefer incremental updates.** Keep each proposal tightly scoped. Large cross-cutting
  programs should be split into separate but linked proposals.

## Review checklist

Reviewers should check:

- [ ] The problem and affected users are clear
- [ ] Goals and non-goals are appropriately scoped
- [ ] The proposal is not engine-specific unless that is explicitly intended
- [ ] Compatibility, configuration, and rollback behavior are addressed
- [ ] Security, resource, and failure-mode impacts are addressed
- [ ] Alternatives are considered and rejected for stated reasons
- [ ] The test plan covers normal behavior and representative failure cases
- [ ] Open questions are explicit and assigned owners where possible

## References

This template draws on practices from:

- Kubernetes Enhancement Proposals (KEPs)
- Python Enhancement Proposals (PEPs)
- Rust RFCs
- Apache Spark Improvement Proposals (SIPs)
