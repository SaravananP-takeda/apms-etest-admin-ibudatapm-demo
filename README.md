# TEST Environment Branch

This branch is used for CI/CD deployment automation.
It is created from 'devops_dev' but intentionally contains only CI/CD artifacts.

## Deployment Process

1. Code changes are made in the source environment branch
2. CI/CD pipeline validates and tests the changes
3. Pipeline creates a PR from source to this branch
4. PR undergoes review and automated checks (CodeQL, security scans)
5. Upon approval, changes are merged and deployed to TEST

## Branch Protection Rules

This branch is protected by:
- Required pull request reviews (minimum 1 approval)
- Required status checks (branch-protection workflows)
- CodeQL security scanning
- Deletion protection
- Force push prevention
