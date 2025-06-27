import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import reactUtils = require("common/reactUtils");
import AiConnectionStrings = require("components/pages/database/aiHub/aiConnectionStrings/AiConnectionStrings");
import AiTasks = require("components/pages/database/aiHub/aiTasks/AiTasks");
import AiAgents = require("components/pages/database/aiHub/aiAgents/AiAgents");
import EditAiAgent = require("components/pages/database/aiHub/aiAgents/edit/EditAiAgent");

export = getAiHubMenuItem;

function getAiHubMenuItem(appUrls: computedAppUrls) {
    const statsItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/ai/connectionStrings',
            moduleId: reactUtils.bridgeToReact(AiConnectionStrings.default, "nonShardedView"),
            title: 'AI Connection Strings',
            nav: true,
            css: 'icon-manage-connection-strings ai-hub',
            dynamicHash: appUrls.aiConnectionStrings
        }),
        new leafMenuItem({
            route: 'databases/ai/agents',
            moduleId: reactUtils.bridgeToReact(AiAgents.default, "nonShardedView"),
            title: 'AI Agents',
            nav: true,
            css: 'icon-ai-agents ai-hub',
            dynamicHash: appUrls.aiAgents
        }),
        new leafMenuItem({
            route: 'databases/ai/agents/edit',
            moduleId: reactUtils.bridgeToReact(EditAiAgent.default, "nonShardedView"),
            title: 'AI Agent',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editAiAgent,
            itemRouteToHighlight: 'databases/ai/agents',
            search: {
                overrideTitle: "Add New AI Agent",
                alternativeTitles: ["Create AI Agent"],
            }
        }),
        new leafMenuItem({
            route: 'databases/ai/tasks',
            moduleId: reactUtils.bridgeToReact(AiTasks.default, "shardedView"),
            shardingMode: "allShards",
            title: 'AI Tasks',
            nav: true,
            css: 'icon-manage-ongoing-tasks ai-hub',
            dynamicHash: appUrls.aiTasks
            
        }),
        new leafMenuItem({
            route: 'databases/ai/tasksStats',
            moduleId: require('viewmodels/database/aiHub/aiTasksStats'),
            shardingMode: "singleShard",
            title: 'AI Tasks Stats',
            nav: true,
            css: 'icon-replication-stats ai-hub',
            dynamicHash: appUrls.aiTasksStats
        }),
    ];

    return new intermediateMenuItem("AI Hub", statsItems, "icon-ai");
}
