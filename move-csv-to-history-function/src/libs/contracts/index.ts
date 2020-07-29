export interface IUser {
  email: string;
  loadDate: string;
}

export interface IRawUser extends IUser {
  eligible: string;

}
export interface ISerializedUser extends IUser {
  eligible: boolean;
}

export type BlacklistState = 'pending' | 'rejected' | 'fulfilled';

export interface IBlacklistStatisticItem {
  filename: string;
  state: BlacklistState;
  loadDate?: string;
  totalCount?: number;
  storedByItemCount?: number;
  srcFilename?: string;
}

export interface IStatisticReport {
  isInProgress: boolean;
}
