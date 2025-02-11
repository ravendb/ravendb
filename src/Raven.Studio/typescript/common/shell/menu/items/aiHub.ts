import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import reactUtils = require("common/reactUtils");
import AiConnectionStrings = require("components/pages/database/aiHub/aiConnectionStrings/AiConnectionStrings");

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
            moduleId: reactUtils.bridgeToReact(AiConnectionStrings.default, "nonShardedView"),
            title: 'AI Tasks',
            nav: true,
            css: 'icon-question',
            dynamicHash: appUrls.aiTasks
            
        }),
        new leafMenuItem({
            route: 'databases/ai/tasksStats',
            moduleId: reactUtils.bridgeToReact(AiConnectionStrings.default, "nonShardedView"),
            title: 'AI Tasks Stats',
            nav: true,
            css: 'icon-question',
            dynamicHash: appUrls.aiTasksStats
        }),
    ];

    return new intermediateMenuItem("AI Hub", statsItems, "icon-question"); // TODO kalczur
}
