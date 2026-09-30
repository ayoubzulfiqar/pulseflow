import { EmbedConfig, EmbedTokenResponse } from "./types";

const DEFAULT_CONFIG: Partial<EmbedConfig> = {
  readOnly: false,
  hideBranding: false,
  defaultTimeRange: "24h",
};

export class PulseFlowEmbedClient {
  private config: EmbedConfig;
  private token: string | null = null;

  constructor(config: EmbedConfig) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  async authenticate(): Promise<string> {
    const res = await fetch(`${this.config.apiUrl}/v1/embed/token?tenant_id=${this.config.tenantId}&read_only=${this.config.readOnly}`, {
      method: "POST",
    });
    if (!res.ok) {
      throw new Error(`Failed to get embed token: ${res.status}`);
    }
    const data: EmbedTokenResponse = await res.json();
    this.token = data.token;
    return this.token;
  }

  private async request(path: string, options: RequestInit = {}) {
    if (!this.token) {
      await this.authenticate();
    }
    const res = await fetch(`${this.config.apiUrl}${path}`, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.token}`,
        ...options.headers,
      },
    });
    if (!res.ok) {
      throw new Error(`API error ${res.status}: ${await res.text()}`);
    }
    return res.json();
  }

  async getDeliveries(limit: number = 50, status?: string): Promise<{ deliveries: any[] }> {
    const params = new URLSearchParams({ limit: String(limit) });
    if (status) params.set("status", status);
    return this.request(`/v1/embed/deliveries?${params.toString()}`);
  }

  async getDestinations(): Promise<{ destinations: any[] }> {
    return this.request(`/v1/embed/destinations`);
  }

  async retryDelivery(destId: string): Promise<{ message: string }> {
    if (this.config.readOnly) {
      throw new Error("Read-only mode: cannot retry deliveries");
    }
    return this.request(`/v1/embed/deliveries/${destId}/retry`, { method: "POST" });
  }

  async getEvents(limit: number = 50, typeFilter?: string): Promise<{ events: any[] }> {
    const params = new URLSearchParams({ limit: String(limit) });
    if (typeFilter) params.set("type", typeFilter);
    return this.request(`/v1/embed/events?${params.toString()}`);
  }
}
