/**
 * @jest-environment jsdom
 */

import databasesManager from "common/shell/databasesManager";
import endpointConstants from "endpoints";
import shardedDatabase from "models/resources/shardedDatabase";
import shard from "models/resources/shard";
import { ajaxMock } from "../../test/mocks";
import { DatabaseStubs } from "../../test/DatabaseStubs";
import nonShardedDatabase from "models/resources/nonShardedDatabase";

describe("databasesManager", () => {
    
    beforeEach(() => {
        jest.clearAllMocks();
    });
    
    it("can handle non-sharded database", async () => {
        const response = DatabaseStubs.singleDatabaseResponse();
        
        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve(response);
            }
        });

        const manager = new databasesManager();
        await manager.init();

        const dbs = manager.databases();
        expect(dbs)
            .toHaveLength(1);

        const firstDb = dbs[0];
        expect(firstDb)
            .toBeInstanceOf(nonShardedDatabase);
        expect(firstDb.name)
            .toEqual(response.Databases[0].Name);
    })
    
    it("can handle sharded database", async () => {
        const response = DatabaseStubs.shardedDatabasesResponse();
        
        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve(response);
            }
        });
        
        const manager = new databasesManager();
        await manager.init();
        
        const dbs = manager.databases();
        
        expect(dbs)
            .toHaveLength(1);
        
        const expectedShardedDatabaseGroup = (response.Databases[0].Name.split("$")[0]);
        
        const firstDb = dbs[0];
        expect(firstDb)
            .toBeInstanceOf(shardedDatabase);
        expect(firstDb.name)
            .toEqual(expectedShardedDatabaseGroup);
        
        const sharded = firstDb as shardedDatabase;
        const shards = sharded.shards();
        expect(shards)
            .toHaveLength(2);
        
        expect(shards[0].name)
            .toEqual(response.Databases[0].Name);
        expect(shards[0])
            .toBeInstanceOf(shard);

        expect(shards[1].name)
            .toEqual(response.Databases[1].Name);
        expect(shards[1])
            .toBeInstanceOf(shard);
    });
    
    it("can get single shard by name", async () => {
        const response = DatabaseStubs.shardedDatabasesResponse();

        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve(response);
            }
        });

        const manager = new databasesManager();
        await manager.init();
        
        const firstShardName = response.Databases[0].Name;
        
        const singleShard = manager.getDatabaseByName(firstShardName) as shard;
        
        expect(singleShard)
            .not.toBeNull();
        expect(singleShard)
            .toBeInstanceOf(shard);
        
        const shardGroup = singleShard.parent;
        expect(shardGroup)
            .not.toBeNull();
        expect(shardGroup)
            .toBeInstanceOf(shardedDatabase);
        expect(shardGroup.shards())
            .toHaveLength(2);
    });

    it("can get sharded database by name", async () => {
        const response = DatabaseStubs.shardedDatabasesResponse();

        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve(response);
            }
        });

        const manager = new databasesManager();
        await manager.init();

        const shardGroupName = response.Databases[0].Name.split("$")[0];

        const shard = manager.getDatabaseByName(shardGroupName) as shardedDatabase;

        expect(shard)
            .not.toBeNull();
        expect(shard)
            .toBeInstanceOf(shardedDatabase);
        expect(shard.shards())
            .toHaveLength(2);
    });
    
    it("can update manager after db was deleted", async () => {
        const response = DatabaseStubs.shardedDatabasesResponse();

        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve(response);
            }
        });

        const manager = new databasesManager();
        await manager.init();

        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve({ 
                    Databases: []
                });
            }
        });
        
        await manager.refreshDatabases();
        
        expect(manager.databases())
            .toHaveLength(0);
    });

    it("can update manager after single shard was deleted", async () => {
        const response = DatabaseStubs.shardedDatabasesResponse();

        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve(response);
            }
        });

        const manager = new databasesManager();
        await manager.init();

        response.Databases.splice(0, 1);
        
        ajaxMock.mockImplementation((args: JQueryAjaxSettings) => {
            if (args.url === endpointConstants.global.databases.databases) {
                return $.Deferred<typeof response>().resolve(response);
            }
        });

        await manager.refreshDatabases();

        expect(manager.databases())
            .toHaveLength(1);
        
        const db1 = manager.getDatabaseByName("db") as shardedDatabase;
        expect(db1.shards())
            .toHaveLength(1);
    })
})


