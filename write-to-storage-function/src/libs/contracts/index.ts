export interface IUser {
  email: string;
  eligible: boolean;
  loadDate: string;
}

export type BlacklistState = 'pending' | 'rejected' | 'fulfilled';

export interface IBlacklistStatisticItem {
  filename: string;
  state: BlacklistState;
  loadDate: string;
}
