/*
requires the following includes to be on page
<link href="css/rdb.jsonEditor.css" rel="Stylesheet" type="text/css" />
<link href="css/smoothness/jquery-ui-1.8rc2.custom.css" rel="Stylesheet" type="text/css" />
<script type="text/javascript" src="js/jquery-1.4.2.min.js"></script>
<script type="text/javascript" src="js/jquery-ui.js"></script>
<script type="text/javascript" src="js/json2.js"></script>
<script type="text/javascript" src="js/jstree/jquery.tree.js"></script>
*/

function InitializeJSONEditor(jsonToEdit) {
    $('#editorContainer').hide().before($('<div id="editorLoading" style="width:600px;">Loading...</div>'));    
    $('#txtJSON').val(JSON.stringify(jsonToEdit, null, '\t'));

    $('#editorTabs').tabs({
        select: function (event, ui) {
			if (ui.index == 0) { //view
                if (ValidateRawJSON()) {
                    $('#jsonViewer').html(GetDocumentViewHTML(JSON.parse($('#txtJSON').val()), $('#txtJSONMetadata').val()));
                } else {
                    event.preventDefault();
                }
            }
        }       
    });
   
    $('#createArray').button({
        icons: { primary: 'ui-icon-folder-collapsed' },
        text: false
    });
    $('#createValue').button({
        icons: { primary: 'ui-icon-document' },
        text: false
    });
    $('#deleteSelected').button({
        icons: { primary: 'ui-icon-closethick' },
        text: false
    });
    
    if (ValidateRawJSON()) {
        $('#jsonViewer').html(GetDocumentViewHTML(JSON.parse($('#txtJSON').val()), $('#txtJSONMetadata').val()));
    } else {
        $('#editorTabs').tabs('select', 1);
    }    
    $('#editorLoading').hide();
    $('#editorContainer').show();
}

function ShowEditorForNewDocument(saveCallback) {
    ShowEditorForDocument(null, { PropertyName: '' }, {}, null, 'Create New Document', function (id, etag, metadata, json, editor) {
        saveCallback(id, metadata, json, editor);
    }, null);
}

function ShowEditorForDocument(id, doc, etag, metadata, title, saveCallback, deleteCallback) {
    var editorHtml = $('<div id="editorContainer"></div>');
    $(editorHtml).load($.ravenDB.getServerUrl() + '/raven/js/rdb.jsonEditor/editor.html', function () {
    	if (id) {
    		$('#documentId', editorHtml).val(id);
    		var deleteButton = $('<button style="margin-top:10px;">Delete Document</button>');
    		$(deleteButton).button({
    			icons: { primary: 'ui-icon-trash' }
    		}).click(function () {
    			$('<div title="Delete Confirmation" class="ui-state-error ui-corner-all" style="padding:20px;"></div>')
                    .html('<p><span style="float: left; margin-right: 0.3em;" class="ui-icon ui-icon-alert"></span>Are you sure you want to delete this document?</p>')
                    .dialog({
                    	modal: true,
                    	buttons: {
                    		'Delete': function () {
                    			deleteCallback(id, etag, editorHtml, this);
                    		},
                    		Cancel: function () {
                    			$(this).dialog('close');
                    		}
                    	},
                    	width: 'auto'
                    });
    		});
    		$(editorHtml).append(deleteButton);
    	}

    	if (metadata) {
    		$(editorHtml).find('#txtJSONMetadata').val(JSON.stringify(metadata, null, '\t'));
    	}
    	else {
    		$(editorHtml).find('#txtJSONMetadata').val('{}');
    	}

    	$(editorHtml).css('position', 'relative').css('height', '500px');
    	$(editorHtml).dialog({
    		modal: true,
    		open: function (event, ui) {
    			InitializeJSONEditor(doc);
    		},
    		close: function () {
    			$('#editorContainer').dialog('destroy');
    			$('#editorContainer').remove();
    		},
    		buttons: {
    			Save: function () {
    				if (ValidateRawJSON() && ValidateMetaData()) {
    					var maybeId = $('#documentId').val();
    					if (maybeId != 'auto generated')
    						id = maybeId;
    					saveCallback(id, etag, JSON.parse($('#txtJSONMetadata').val()), GetJSONFromEditor(), editorHtml);
    				};
    			},
    			Cancel: function () {
    				$(this).dialog('close');
    			}
    		},
    		title: title,
    		width: 'auto'
    	});

    });
}

function ValidateMetaData() {
    try {
        var json = JSON.parse($('#txtJSONMetadata').val());
        if (typeof json != "object")
            throw "Invalid";
        return true;
    }
    catch (err) {
        $('<div title="Invalid Metadata">There was an error parsing the Document Metadata - Please enter valid JSON before proceeding.</div>').
            dialog({
                modal: true,
                buttons: {
                    Ok: function () {
                        $(this).dialog('close');
                    }
                }
            });
        return false;
    }
}

function ValidateRawJSON() {
    try
    {
        var json = JSON.parse($('#txtJSON').val());
        if (typeof json != "object")
            throw "Invalid";
        return true;
    }
    catch (err)
    {
        $('<div title="Invalid JSON">There was an error parsing the JSON data.  Please enter valid JSON before proceeding.</div>').
            dialog({
                modal: true,
                buttons: { 
                    Ok: function() {
                        $(this).dialog('close');
                    }
                }
            });
        return false;
    }
}

function GetJSONFromEditor() {
        return $('#txtJSON').val();        
}
