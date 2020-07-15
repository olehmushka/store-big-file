export interface IError {
  message: string;
}

export abstract class CustomError extends Error {
  constructor({ message }: IError) {
    super(message);
  }
}