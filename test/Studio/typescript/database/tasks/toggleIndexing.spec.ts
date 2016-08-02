import chai = require("chai");
import utils = require("utils");

import viewModel = require("src/Raven.Studio/typescript/viewmodels/database/tasks/toggleIndexing");

var viewUnderTest = 'database/tasks/toggleIndexing';

describe(viewUnderTest, () => {

    utils.initTest();

    it('should bind', () => {
        utils.mockCommand('commands/database/index/getIndexingStatusCommand', {
            "MappingStatus": "Mapping",
            "ReducingStatus": "Reducing"
        });

        return utils.runViewmodelTest(viewUnderTest, {
            afterAttach: (vm: viewModel) => {
                chai.expect(vm.isMappingEnabled()).to.be.true;
                chai.expect(vm.isReducingEnabled()).to.be.true;
                chai.expect(vm.indexingStatusText()).to.equal("Mapping & Reducing");
            }
        });
    });

    it('should update paused state', () => {
        utils.mockCommand('commands/database/index/getIndexingStatusCommand', {
            "MappingStatus": "Paused",
            "ReducingStatus": "Paused"
        });

        return utils.runViewmodelTest(viewUnderTest, {
            afterAttach: (vm: viewModel) => {
                chai.expect(vm.isMappingEnabled()).to.be.false;
                chai.expect(vm.isReducingEnabled()).to.be.false;
                chai.expect(vm.indexingStatusText()).to.equal("Paused");
            }
        });
    });
});

