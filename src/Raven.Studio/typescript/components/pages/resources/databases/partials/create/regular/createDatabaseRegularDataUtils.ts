import { CreateDatabaseRegularFormData as FormData } from "./createDatabaseRegularValidation";
import { CreateDatabaseDto } from "commands/resources/createDatabaseCommand";

const getDefaultValues = (replicationFactor: number): FormData => {
    return {
        basicInfo: {
            databaseName: "",
            isEncrypted: false,
        },
        encryption: {
            key: "",
            isKeySaved: false,
        },
        replicationAndSharding: {
            replicationFactor,
            isSharded: false,
            shardsCount: 1,
            isDynamicDistribution: false,
            isManualReplication: false,
        },
        manualNodeSelection: {
            nodes: [],
            shards: [],
        },
        pathsConfigurations: {
            isDefault: true,
            path: "",
        },
    };
};

function mapToDto(formValues: FormData, allNodeTags: string[]): CreateDatabaseDto {
    const { basicInfo, replicationAndSharding, manualNodeSelection, pathsConfigurations } = formValues;

    const Settings: CreateDatabaseDto["Settings"] = pathsConfigurations.isDefault
        ? {}
        : {
              DataDir: _.trim(pathsConfigurations.path),
          };

    const Topology: CreateDatabaseDto["Topology"] = replicationAndSharding.isSharded
        ? null
        : {
              Members: replicationAndSharding.isManualReplication ? manualNodeSelection.nodes : null,
              DynamicNodesDistribution: replicationAndSharding.isDynamicDistribution,
          };

    const Shards: CreateDatabaseDto["Sharding"]["Shards"] = {};

    if (replicationAndSharding.isSharded) {
        for (let i = 0; i < replicationAndSharding.shardsCount; i++) {
            Shards[i] = replicationAndSharding.isManualReplication
                ? {
                      Members: manualNodeSelection.shards[i],
                  }
                : {};
        }
    }

    const selectedOrchestrators =
        formValues.replicationAndSharding.isSharded && formValues.replicationAndSharding.isManualReplication
            ? formValues.manualNodeSelection.nodes
            : allNodeTags;

    const Sharding: CreateDatabaseDto["Sharding"] = replicationAndSharding.isSharded
        ? {
              Shards,
              Orchestrator: {
                  Topology: {
                      Members: selectedOrchestrators,
                  },
              },
          }
        : null;

    return {
        DatabaseName: basicInfo.databaseName,
        Settings,
        Disabled: false,
        Encrypted: basicInfo.isEncrypted,
        Topology,
        Sharding,
    };
}

export const createDatabaseRegularDataUtils = {
    getDefaultValues,
    mapToDto,
};
