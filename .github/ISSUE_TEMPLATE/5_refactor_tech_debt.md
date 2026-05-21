---
name: Refactor & Tech Debt
about: Identify areas of the codebase that need refactoring, decoupling, or cleanup.
title: '[REFACTOR] '
labels: 'refactor, tech-debt'
assignees: ''
---

## Tech Debt Area
What code, module, configuration, or database structure is being targeted for refactoring?

## Reason for Refactoring
Why is this refactor necessary? Choose all that apply:
- [ ] **Readability / Clean Code** (hard to follow logic, overly long functions)
- [ ] **Decoupling / Reusability** (redundant code, lack of modularity)
- [ ] **Configurability** (hardcoded paths, credential issues, env management)
- [ ] **Testing** (difficult to unit test the component in isolation)
- [ ] **Other** (please specify)

Please provide a detailed description of the current issue:

## Proposed Refactoring Strategy
How should we rewrite or reorganize the code? Provide code structures, interface details, or architecture ideas if possible.

## Action Plan / Tasks
Checklist of items to complete:
- [ ] Step 1
- [ ] Step 2
- [ ] Step 3

## Verification Plan
How will we verify that the refactoring did not break existing functionality? (e.g., "Run PySpark deduplication test suite", "Verify ClickHouse schema matches exactly after refactor").
