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
    $('#txtJSON').val(JSON.stringify(jsonToEdit));

    $('#editorTabs').tabs({
        select: function (event, ui) {
            if (ui.index == 2) { //raw 
                if ($('#editorTabs').find(".ui-tabs-panel:visible").attr('id') == 'fancyJSONEditor') {
                    var treeJSON = GetJSONFromTree();
                    $('#txtJSON').val(treeJSON);
                }
            } else if (ui.index == 1) { //edit
                if (ValidateRawJSON()) {
                    var json = JSON.parse($('#txtJSON').val());
                    LoadJSONToTree(json);
                } else {
                    event.preventDefault();
                }
            } else if (ui.index == 3) { //view template
                //update json in json editor
                if ($('#editorTabs').find(".ui-tabs-panel:visible").attr('id') == 'fancyJSONEditor') {
                    var json = GetJSONFromTree();
                    $('#txtJSON').val(json);
                }            
            } else if (ui.index == 0) { //view
                if ($('#editorTabs').find(".ui-tabs-panel:visible").attr('id') == 'fancyJSONEditor') {
                    var json = GetJSONFromTree();
                    $('#txtJSON').val(json);
                }
                if (ValidateRawJSON()) {
                    $('#jsonViewer').html(GetDocumentViewHTML(JSON.parse($('#txtJSON').val()), $('#txtJSONViewTemplate').val()));
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
    
    $('#editorApplyChanges, #editorApplyChanges2').button({
        icons: { primary: 'ui-icon-disk' }
    }).click(function () {
        UpdateSelectedNode(); 
    });
    
    if (ValidateRawJSON()) {
        $('#jsonViewer').html(GetDocumentViewHTML(JSON.parse($('#txtJSON').val()), $('#txtJSONViewTemplate').val()));
    } else {
        $('#editorTabs').tabs('select', 1);
    }    
    $('#editorLoading').hide();
    $('#editorContainer').show();
}

function ShowEditorForNewDocument(saveCallback) {
    ShowEditorForDocument(null, { PropertyName : ''}, null, null, 'Create New Document', function(id, etag, template, json, editor) {
        saveCallback(template, json, editor);
    }, null);
}

function ShowEditorForDocument(id, doc, etag, viewTemplate, title, saveCallback, deleteCallback) {
    var editorHtml = $('<div id="editorContainer"></div>');
    $(editorHtml).load('/divan/js/rdb.jsonEditor/editor.html', function() {
        if (id) {
            var deleteButton = $('<button style="margin-top:10px;">Delete Document</button>');
            $(deleteButton).button({
                icons: { primary: 'ui-icon-trash' }
            }).click(function() {
                $('<div title="Delete Confirmation" class="ui-state-error ui-corner-all" style="padding:20px;"></div>')
                    .html('<p><span style="float: left; margin-right: 0.3em;" class="ui-icon ui-icon-alert"></span>Are you sure you want to delete this document?</p>')
                    .dialog({
                        modal: true,
                        buttons: { 
                            'Delete' : function() {
                                deleteCallback(id, etag, editorHtml, this);                                                              
                            }, 
                            Cancel: function() {
                                $(this).dialog('close');
                            }
                        },
                        width: 'auto'
                    });
            });
            $(editorHtml).append(deleteButton);
            
            if (viewTemplate) 
                $(editorHtml).find('#txtJSONViewTemplate').val(viewTemplate);
        }
        $(editorHtml).css('position', 'relative').css('height', '500px');
        $(editorHtml).dialog({
        modal: true,
        open: function(event, ui) {
            InitializeJSONEditor(doc);
        },
        close: function() {
            $('#editorContainer').dialog('destroy');
            $('#editorContainer').remove();
        },
        buttons: {
            Save: function () {
                if (ValidateRawJSON()) {
                    saveCallback(id, etag, $('#txtJSONViewTemplate').val(), GetJSONFromEditor(), editorHtml);
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
        $('<div title="Invalid JSON">There was an error parsing the JSON value.  Please enter valid JSON before proceeding.</div>').
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
    if ($('#editorTabs').find(".ui-tabs-panel:visible").attr('id') == 'fancyJSONEditor') {
        UpdateSelectedNode(); 
        return GetJSONFromTree();        
    } else { //edit
        return $('#txtJSON').val();        
    } 
}

function LoadTree(json) {
    $('#jsonTree').tree({
        data: {
            type: 'json',
            opts: {
                static: json
            }
        },
        types: {
            "default": {
                clickable: true,
                renameable: true,
                deletable: true,
                creatable: true,
                draggable: true,
                max_children: -1,
                max_depth: -1,
                valid_children: "all",

                icon: {
                    image: false,
                    position: false
                }
            },
            "jsonValue": {
                valid_children: "none",
                max_children: 0,
                max_depth: 0,
                renameable: true,
                icon: {
                    image: 'js/jstree/themes/icons/txt.png'
                }
            }
        },
        callback: {
            onselect: function (node, tree) {
                if (tree.get_type(node) == 'jsonValue') {
                    $('#selectedJSONname').val($.trim($(node).children('a').text()));
                    $('#selectedJSONval').val(unescape($(node).attr('jsonvalue')));
                    if ($('#jsonArrayEditor').is(':visible')) {
                        $('#jsonArrayEditor').fadeOut('slow', function () {
                            $('#jsonEditor').fadeIn('slow', function() {
                                $('#selectedJSONname').focus().select();
                            });
                        });
                    } else {
                        $('#jsonEditor').fadeIn('fast', function() {
                            $('#selectedJSONname').focus().select();
                        });
                    }

                } else {
                    $('#selectedJSONArrayName').val($(node).children('a').text().trim());
                    if ($('#jsonEditor').is(':visible')) {
                        $('#jsonEditor').fadeOut('slow', function () {
                            $('#jsonArrayEditor').fadeIn('slow', function() {
                                $('#selectedJSONArrayName').focus().select();
                            });
                        });
                    } else {
                        $('#jsonArrayEditor').fadeIn('fast', function() {
                            $('#selectedJSONArrayName').focus().select();
                        });
                    }
                }
            },
            onload: function (tree) {
                $.tree.focused().select_branch($.tree.focused().container.find('.leaf:first a:first'));
            }
        }
    });
}

function CreateArray() {
    if ($.tree.focused().get_type(node) == 'jsonValue') {
        var node = $.tree.focused().create({
            data: 'New Array',
            attributes: { rel: 'default' }
        }, $.tree.focused().selected, 'after');
        if (node) {
            $.tree.focused().select_branch(node);
        }                
    } else {
        var node = $.tree.focused().create({ data: 'New Array' });
        if (node) {
            $.tree.focused().select_branch(node);
        }
    }

}

function CreateValue() {
    if ($.tree.focused().get_type(node) == 'jsonValue') {
        var node = $.tree.focused().create({
            data: 'New Value',
            attributes: { rel: 'jsonValue', "jsonvalue": '' }
        }, $.tree.focused().selected, 'after');
        if (node) {
            $.tree.focused().select_branch(node);
        }  
    } else {
        var node = $.tree.focused().create({
            data: 'New Value',
            attributes: { rel: 'jsonValue', "jsonvalue": '' }
        });
        if (node) {
            $.tree.focused().select_branch(node);
        }
    }
}

function LoadJSONToTree(json) {
    json = { Document : json };
    json = JSONToTreeJSON(json, '');
    LoadTree(json);
}

function GetJSONFromTree() {
    //tree.get only gives values for open elements, expand all
    $.tree.focused().open_all();
    var treeJSON = $.tree.focused().get(null, null, { outer_attrib: ["jsonvalue"] });
    var convertedTreeJSON = traverseTreeJSON(treeJSON);
    convertedTreeJSON = convertedTreeJSON.Document;
    var arrayAsJSON = {};
    $(convertedTreeJSON).each(function() {
        $.each(this, function(key, value) {
            arrayAsJSON[key] = value;
        });
    });
    var jsonString = JSON.stringify(arrayAsJSON, null, '\t');
    return jsonString;
}


function traverseTreeJSON(json) {
    var retJSON = {};
    if (json.children) {
        var childrenJSON = [];
        $(json.children).each(function () {
            var childJSON = traverseTreeJSON(this);
            $.each(childJSON, function (key, value) {
                var childPair = {};
                childPair[key] = value;
                childrenJSON.push(childPair);
            });
        });

        if (json.data.title && json.data.title != '') {
            retJSON[json.data.title] = childrenJSON;
        }
    } else {
        if (json.data.title && json.data.title != '') {
            retJSON[json.data.title] = unescape(json.attributes.jsonvalue);
        }
    }
    return retJSON;
}

function JSONToTreeJSON(jsonObj) {
    if (typeof jsonObj == "object") {
        var jsonArr = [];
        $.each(jsonObj, function (key, value) {            
            if (value) {
                // key is either an array index or object key                    
                var children = JSONToTreeJSON(value);

                if (IsArray(jsonObj)) {
                    jsonArr.push(children);
                } else if (typeof children == "object") {
                    jsonArr.push({
                        data: key,
                        children: children
                    });
                } else {
                    jsonArr.push({
                        data: key,
                        attributes: { rel : 'jsonValue',  "jsonvalue": escape(children) }
                    });
                }
            } else {
                jsonArr.push({
                    data: key,
                    attributes: { rel : 'jsonValue', "jsonvalue": '' }
                });
            }
        });
        return jsonArr;
    }
    else {
        // jsonOb is a number or string
        return jsonObj;
    }
}

function UpdateSelectedNode() {
    if ($.tree.focused().get_type($.tree.focused().selected) == 'jsonValue') {
        $.tree.focused().rename($.tree.focused().selected, $('#selectedJSONname').val());
        $.tree.focused().selected.attr('jsonvalue', escape($('#selectedJSONval').val()));
    } else {
        $.tree.focused().rename($.tree.focused().selected, $('#selectedJSONArrayName').val());
    }
}

function IsArray(obj) {
   if (obj.constructor.toString().indexOf("Array") == -1)
      return false;
   else
      return true;
}