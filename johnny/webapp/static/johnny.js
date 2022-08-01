// Common JavaScript code.
//
//  Copyright (C) 2021  Martin Blais
//  License: GNU GPLv2

function CreateChainsTable(id, extra_config) {
    // A mapping of columns with reducers for the bottom line.
    const sum_columns = {
        init: SumFloat,
        init_legs: SumInteger,
        pnl_chain: SumFloat,
        pnl_win: SumFloat,
        pnl_loss: SumFloat,
        net_liq: SumFloat,
        commissions: SumFloat,
        fees: SumFloat,
        credits: SumFloat,
    }

    // Create the table instance.
    var config = {
        pageLength: 200,
        select: 'multi+shift',
        fixedHeader: true,
        colReorder: true,
        columnDefs: [
            {targets: ['init', 'pnl_chain', 'pnl_win', 'pnl_loss',
                       'pnl_frac', 'target', 'pop',
                       'net_win', 'net_liq', 'net_loss', 'fifo_cost',
                       'commissions', 'fees',
                       'vol_real', 'return_real', 'stdev_real',
                       'vol_impl', 'return_impl', 'stdev_impl'],
             className: 'dt-body-right'},
        ],

        // This gets called once when the table is drawn.
        footerCallback: function (row, data, start, end, display) {
            var api = this.api()
            UpdateFooter(api, sum_columns);
        }
    };
    if (extra_config != null) {
        config = Object.assign(config, extra_config)
    }
    var table = $(id).DataTable(config);

    // Emphasize some columns of the table.
    $(table.column(':contains(pnl_win)').nodes()).addClass('win-column');
    $(table.column(':contains(pnl_chain)').nodes()).addClass('pnl-column');
    $(table.column(':contains(pnl_loss)').nodes()).addClass('loss-column');
    $(table.column(':contains(net_win)').nodes()).addClass('win-column');
    $(table.column(':contains(net_liq)').nodes()).addClass('pnl-column');
    $(table.column(':contains(net_loss)').nodes()).addClass('loss-column');

    InstallDataTableFocus(table);

    AddFooterSums(table, sum_columns);

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

function SumFloat(a, b) {
    return a + b;
}

function SumInteger(a, b) {
    return a + b;
}

function toFloat(s) {
    return parseFloat(s.replace(/,/g, ""));
}

// Make the table render footer sums.
function AddFooterSums(table, sum_columns) {
    table.on('select.dt', function () {
        UpdateFooter(table, sum_columns);
    });
    table.on('deselect.dt', function () {
        UpdateFooter(table, sum_columns);
    });
}

// Update the footer sums on a selection change.
function UpdateFooter(table, sum_columns) {
    $.each(sum_columns, function(colname, reducer) {
        var contains = ':contains(' + colname + ')';
        var column = table.column(contains);
        var data = table.cells('.selected', contains).data();
        if (data.length == 0) {
            data = table.cells({search: 'applied'}, contains).data();
            if (data.length == 0) {
                $(column.footer()).html('N/A');
                return;
            }
        }

        var sum = $.map(data, toFloat).reduce(reducer);
        $(column.footer()).html(sum.toLocaleString('en-US'));
    });
}
