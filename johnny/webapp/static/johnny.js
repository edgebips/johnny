// Common JavaScript code.
//
//  Copyright (C) 2021  Martin Blais
//  License: GNU GPLv2


// Bind SLASH to focus on the search box of the DataTable instance from
// anywhere.
function InstallDataTableFocus(table) {

    // Install a handler for focusing on a key pressa.
    $(document).keypress(function(ev) {
        var tdiv = table.table().container()
        var search_input = $(tdiv).find(".dataTables_filter input");
        // console.log(search_input);
        if (ev.which == 47 && ev.key == '/')  {
            if (!search_input.is(":focus")) {
                event.preventDefault();
                search_input.focus();
            }
        }
    });
}

// TODO(blais): Clean this up.

    // // Add column name as class to the column
    // $(table.table().header()).find('th').each(function (_) {
    //     ////console.log($(this).text(), $(this).index(), $(this));
    //     $(this).addClass($(this).text());
    // });

// // Find a column index by header name.
// function FindColumn(table, name) {
//     var header = table.table().header()
//     return $(header).find('th:contains("' + name + '")').index();
// }

// function SelectedChains(table) {
//     var index = FindColumn(table, "chain_id");
//     return table.rows('.selected').data();
// }
