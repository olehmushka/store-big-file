export interface ILogger {
  info(...args: any[]): any;
  debug(...args: any[]): any;
  warn(...args: any[]): any;
  error(...args: any[]): any;
}
