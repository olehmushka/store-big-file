export interface IUser {
  email: string;
  eligible: boolean;
  loadDate: string;
}

export interface ICsvUploadStreamResult {
  recordCount: number;
}
