import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import reactUtils = require("common/reactUtils");
import AiConnectionStrings = require("components/pages/database/aiHub/aiConnectionStrings/AiConnectionStrings");
import AiTasks = require("components/pages/database/aiHub/aiTasks/AiTasks");

export = getAiHubMenuItem;

function getAiHubMenuItem(appUrls: computedAppUrls) {
    const statsItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/ai/connectionStrings',
            moduleId: reactUtils.bridgeToReact(AiConnectionStrings.default, "nonShardedView"),
            title: 'AI Connection Strings',
            nav: true,
            css: 'icon-question',
            dynamicHash: appUrls.aiConnectionStrings
        }),
        new leafMenuItem({
            route: 'databases/ai/tasks',
            moduleId: reactUtils.bridgeToReact(AiTasks.default, "shardedView"),
            shardingMode: "allShards",
            title: 'AI Tasks',
            nav: true,
            css: 'icon-question',
            dynamicHash: appUrls.aiTasks
            
        }),
        new leafMenuItem({
            route: 'databases/ai/tasksStats',
            moduleId: require('viewmodels/database/aiHub/aiTasksStats'),
            shardingMode: "singleShard",
            title: 'AI Tasks Stats',
            nav: true,
            css: 'icon-question',
            dynamicHash: appUrls.aiTasksStats
        }),
    ];

    return new intermediateMenuItem("AI Hub", statsItems, "icon-question"); // TODO kalczur
}
