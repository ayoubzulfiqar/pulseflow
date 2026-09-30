export type EmbedConfig = {
  apiUrl: string;
  tenantId: string;
  readOnly?: boolean;
  hideBranding?: boolean;
  theme?: EmbedTheme;
  defaultTimeRange?: string;
};

export type EmbedTheme = {
  primaryColor?: string;
  accentColor?: string;
  logoUrl?: string;
  borderRadius?: string;
  font?: string;
};

export type DeliveryStatus = "delivering" | "delivered" | "failed" | "filtered" | "disabled";

export type AuditRecord = {
  id: string;
  event_id: string;
  destination_id: string;
  timestamp: string;
  status: DeliveryStatus;
  status_code: number;
  reason: string;
  redacted: boolean;
};

export type Destination = {
  id: string;
  url: string;
  event_pattern: string;
  status: "active" | "disabled";
  rate_limit_rps: number;
  concurrency_limit: number;
  cel_filter: string;
};

export type EmbedTokenResponse = {
  token: string;
  tenant_id: string;
  expires_at: number;
  read_only: boolean;
};
