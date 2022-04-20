import { AutoMockService } from "./AutoMockService";
import DatabasesService from "../../components/services/DatabasesService";

export default class MockDatabasesService extends AutoMockService<DatabasesService> {
    constructor() {
        super(new DatabasesService());
    }
}
