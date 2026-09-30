# PulseFlow Embed

Embeddable white-label webhook management component for SaaS platforms.

## Installation

```bash
npm install @pulseflow/embed
# or
yarn add @pulseflow/embed
# or
pnpm add @pulseflow/embed
```

## Usage (React)

```tsx
import { PulseFlowEmbed, PulseFlowEmbedStyles } from "@pulseflow/embed";

// Include styles once in your app.
import "@pulseflow/embed/dist/styles.css";

function MyApp() {
  return (
    <PulseFlowEmbed
      config={{
        apiUrl: "https://api.yourapp.com", // PulseFlow API endpoint
        tenantId: "tenant_abc123",        // Scoped to this tenant
        readOnly: false,                  // Allow retry operations
        hideBranding: true,               // Hide "Powered by PulseFlow"
        theme: {
          primaryColor: "#3b82f6",
          accentColor: "#10b981",
          logoUrl: "https://yourapp.com/logo.png",
          borderRadius: "12px",
          font: "Inter, sans-serif",
        },
      }}
      onReady={() => console.log("Embed loaded")}
    />
  );
}
```

## Usage (Vanilla JS / iframe)

```html
<!-- Option 1: Direct React bundle -->
<script src="https://cdn.yourapp.com/pulseflow-embed.js"></script>
<link rel="stylesheet" href="https://cdn.yourapp.com/pulseflow-embed.css" />
<div id="pulseflow-root"></div>
<script>
  window.PulseFlowEmbed.render({
    apiUrl: "https://api.yourapp.com",
    tenantId: "tenant_abc123",
    container: "#pulseflow-root",
    hideBranding: true,
    theme: { primaryColor: "#3b82f6" },
  });
</script>

<!-- Option 2: iframe (cross-origin safe) -->
<iframe
  src="https://embed.yourapp.com?tenant_id=tenant_abc123&read_only=false&primary_color=3b82f6"
  width="100%"
  height="600"
  frameborder="0"
  style="border: none; border-radius: 8px;"></iframe>
```

## API Endpoints

All endpoints are under `/v1/embed/` and require a valid embed token.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/embed/token` | Generate a short-lived embed token (tenant-scoped) |
| `GET`  | `/v1/embed/destinations` | List webhook destinations for the tenant |
| `GET`  | `/v1/embed/deliveries` | List recent delivery/audit records |
| `GET`  | `/v1/embed/events` | List recent events for the tenant |
| `POST` | `/v1/embed/deliveries/:id/retry` | Retry a failed delivery |

## Configuration

Embed is enabled in `config.yaml`:

```yaml
embed:
  enabled: true
  token_secret: "your-hmac-secret-for-token-signing"
  token_ttl: 24h
```

## Features

- **Tenant isolation** — all data is automatically scoped to the caller's tenant ID
- **Brand controls** — customizable colors, logo, font, and corner radius
- **Read-only mode** — disable write operations when needed
- **Responsive** — works in iframes of any width
- **Zero external CSS dependencies** — styles are self-contained
