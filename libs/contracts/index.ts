export interface ICSVPubSubPayload {
  data: {
    csvFilename: string;
  };
}

export interface IUser {
  email: string;
  eligible: boolean;
  loadDate: string;
}
