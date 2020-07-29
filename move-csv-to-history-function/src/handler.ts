import { Either, left, right } from '@sweet-monads/either';
import { IFirestoreClient } from './libs/firestore-client';
import { IStorageClient } from './libs/storage-client';
import { ILogger } from './libs/logger';
import { IBlacklistStatisticItem, IStatisticReport } from './libs/contracts';
import config from './config';

export class MoveCsvToHistoryHandler {
  constructor(
    private logger: ILogger,
    private statisticFirestoreClient: IFirestoreClient,
    private srcStorageClient: IStorageClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error, void>> {
    const statisticReport = await this.getStatisticReport();
    if (statisticReport.isLeft()) {
      return left(statisticReport.value);
    }

    this.logger.debug('Started checking if some file is processed');
    if (statisticReport.value.isInProgress) {
      this.logger.debug('Another file is in progress for now');

      return this.markAsIgnored(filename);
    }
    this.logger.debug(`File ${filename} can be precessed for now`);

    const moveInputFileToHistoryResult = await this.moveInputFileToHistory(filename);
    if (moveInputFileToHistoryResult.isLeft()) {
      return moveInputFileToHistoryResult;
    }
    this.logger.debug(`Moved ${filename} from ${config.SRC_CSV_BACKET} to ${config.HISTORY_CSV_BACKET}`);

    return right(undefined);
  }

  private async getStatisticReport(): Promise<Either<Error, IStatisticReport>> {
    const outputFilenames = await this.statisticFirestoreClient.get<IBlacklistStatisticItem>({
      fieldName: 'state',
      operation: '==',
      valueToCompare: 'pending',
    });
    if (outputFilenames.isLeft()) {
      return left(outputFilenames.value);
    }

    return right({
      isInProgress: outputFilenames.value.length !== 0,
    });
  }

  private async markAsIgnored(srcFilename: string): Promise<Either<Error, void>> {
    const fileExtension = '.csv';
    const [filenameBase] = srcFilename.split(fileExtension);
    const ignoredSourceFilename = `${filenameBase}${config.IGNORED_FILE_SUFFIX}${fileExtension}`;

    const renamingSourceFileResult = await this.srcStorageClient.renameFile(
      srcFilename,
      ignoredSourceFilename,
    );
    if (renamingSourceFileResult.isLeft()) {
      return renamingSourceFileResult;
    }
    this.logger.debug(
      `Renamed ${srcFilename} to ${ignoredSourceFilename} in ${config.SRC_CSV_BACKET} bucket`,
    );

    return right(undefined);
  }

  private moveInputFileToHistory(filename: string): Promise<Either<Error, void>> {
    return this.srcStorageClient.moveFileToBucket(config.HISTORY_CSV_BACKET, filename);
  }
}
