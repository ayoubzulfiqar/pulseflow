# Security Policy

## Supported Versions

Use this section to tell people which versions of PulseFlow are currently
being supported with security updates.

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |
| 1.x.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security
vulnerability in PulseFlow, please report it responsibly.

**Do not open a public GitHub issue.** Instead, email directly to:

**contact@ayoubzulfiqar.com**

Include the following information:
- A description of the vulnerability
- Steps to reproduce
- Impact assessment (if known)
- Your contact information (for follow-up)

We will acknowledge receipt within 24 hours and provide a detailed response
within 72 hours. If the vulnerability is confirmed, we will work with you to
release a fix and coordinate disclosure.

## Security Considerations

PulseFlow takes the following security measures:

- **Webhook Signing**: All webhook payloads are signed with HMAC-SHA256.
- **PII Redaction**: Sensitive data can be automatically redacted before
  storage or delivery (Compliance wedge).
- **Immutable Audit Trail**: All delivery attempts are signed with HMAC-SHA256
  for verifiable audit records.
- **Input Validation**: All events are validated before persistence.
- **Rate Limiting**: Per-IP and per-tenant rate limiting is enforced.
- **ULID Identifiers**: Prevents sequential ID enumeration.

## Dependency Security

- All dependencies are pinned in `go.mod` and `go.sum`.
- Run `go mod verify` to ensure module integrity.
- Regularly review `go list -m all` for outdated dependencies.

## Security Headers

All API responses include:
- `Content-Type: application/json`
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`

## Acknowledgements

We appreciate the security research community's efforts in responsibly
disclosing vulnerabilities. Thank you for helping keep PulseFlow secure.
