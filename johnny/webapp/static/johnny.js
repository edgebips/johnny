// Common JavaScript code.
//
//  Copyright (C) 2021  Martin Blais
//  License: GNU GPLv2

function CreateChainsTable(id) {
    // Create the table instance.
    var config = {
        pageLength: 200,
        select: 'multi+shift',
        fixedHeader: true,
        colReorder: true,
        columnDefs: [{
            // TODO(blais): Set this by name (doesn't seem to work).
            // These are the column indices with numbers, to right-align.
            targets: [6,7,8,9,10,11],
            className: 'dt-body-right'
        }],

        // This gets called once when the table is drawn.
        ///footerCallback: function (row, data, start, end, display) {}
    };
    var table = $(id).DataTable(config);

    // Emphasize some columns of the table.
    $(table.column(':contains(pnl_chain)').nodes()).addClass('emph-column');

    table.on('select.dt', function () {
        UpdateFooter(table);
    });
    table.on('deselect.dt', function () {
        UpdateFooter(table);
    });

    InstallDataTableFocus(table);

    return table;
}

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

// Update the footer sums on a selection change.
function UpdateFooter(table) {
    const columns = {
        init: sumFloat,
        init_legs: sumInt,
        pnl_chain: sumFloat,
        net_liq: sumFloat,
        commissions: sumFloat,
        fees: sumFloat,
    }
    $.each(columns, function(colname, reducer) {
        var data = table.cells('.selected', ':contains(' + colname + ')').data();
        var column = table.column(':contains(' + colname + ')');
        if (data.length == 0) {
            $(column.footer()).html('');
            return;
        }
        var sum = data.reduce(reducer);
        $(column.footer()).html(sum);
    });
}
