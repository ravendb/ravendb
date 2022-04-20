/// <reference path="../../../typings/tsd.d.ts" />

import getDatabasesCommand from "commands/resources/getDatabasesCommand";
import saveDatabaseLockModeCommand from "commands/resources/saveDatabaseLockModeCommand";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { DatabaseSharedInfo } from "../models/databases";

export default class DatabasesService {
    async getDatabases() {
        return new getDatabasesCommand().execute();
    }

    async setLockMode(db: DatabaseSharedInfo, newLockMode: DatabaseLockMode) {
        return new saveDatabaseLockModeCommand([db], newLockMode).execute();
    }
}
