---
name: IaC & CI/CD
about: Standard tasks for infrastructure setup, deployments, and CI/CD pipelines.
title: '[INFRA] '
labels: 'devops, infrastructure'
assignees: ''
---

## DevOps / Infrastructure Task
Provide a clear description of the infrastructure or automation setup needed (e.g., "Set up GitHub Actions to lint Python with Ruff and run Docker Compose tests", "Create a staging ClickHouse instance").

## Goal & Purpose
Why is this setup required? What value does it add to the pipeline lifecycle? (e.g., "Prevent syntax errors and test breaking database DDL changes before merging to main").

## Proposed Components
Check all elements of the infrastructure stack that will be added or modified:
- [ ] **Docker / Compose** (multi-container orchestration, volume mappings)
- [ ] **GitHub Actions** (CI/CD workflows, automation scripts)
- [ ] **Environment variables / Configs** (`.env` setup, credential handling)
- [ ] **Cloud / Deployment Infrastructure** (AWS, GCP, Terraform)
- [ ] **Scheduler / Orchestration Engine** (Airflow environment setup)
- [ ] **Other** (please specify)

## Implementation Steps / Requirements
Outline the tasks or requirements for the DevOps change:
- [ ] Step 1: Add a workflow configuration file at `.github/workflows/...`
- [ ] Step 2: Configure repository secrets.
- [ ] Step 3: Verify execution logs.

## Verification & Testing
How will we test this infrastructure change? (e.g., "Run local Makefile commands", "Trigger GitHub Action workflow manually and verify it passes").
