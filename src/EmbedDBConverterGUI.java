/**
 * @file		EmbedDBConverterGUI.java
 * @author		EmbedDB Team
 * @brief		Source code for EmbedDB-SQL
 * @copyright	Copyright 2023
 * 			    EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
 */

import SQLConverter.SQLConverter;

import javax.swing.*;
import java.awt.*;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.util.ArrayList;

public class EmbedDBConverterGUI {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(EmbedDBConverterGUI::createAndShowGUI);
    }

    private static void createAndShowGUI() {
        int width = 600;

        JFrame frame = new JFrame("EmbedDB SQL Converter");
        Container pane = frame.getContentPane();
        pane.setLayout(new BoxLayout(pane, BoxLayout.Y_AXIS));

        JTextArea ddlTextArea = new JTextArea();
        ddlTextArea.setLineWrap(true);
        ddlTextArea.setBorder(BorderFactory.createEtchedBorder());
        JLabel ddlLabel = new JLabel("SQL DDL");
        setLabelStyle(ddlLabel);
        ddlLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
        setComponentFont(ddlTextArea);
        addPadding(ddlTextArea, 5, 5, 5, 5);

        JTextArea queryTextArea = new JTextArea();
        queryTextArea.setLineWrap(true);
        queryTextArea.setBorder(BorderFactory.createEtchedBorder());
        JLabel queryLabel = new JLabel("SQL Query");
        setLabelStyle(queryLabel);
        queryLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
        setComponentFont(queryTextArea);
        addPadding(queryTextArea, 5, 5, 5, 5);

        JPanel ddlPanel = new JPanel();
        ddlPanel.setLayout(new BoxLayout(ddlPanel, BoxLayout.Y_AXIS));
        addPadding(ddlLabel, 0, 0, 8, 0);
        ddlPanel.add(ddlLabel);
        ddlPanel.add(ddlTextArea);

        JPanel queryPanel = new JPanel();
        queryPanel.setLayout(new BoxLayout(queryPanel, BoxLayout.Y_AXIS));
        addPadding(queryLabel, 0, 0, 8, 0);
        queryPanel.add(queryLabel);
        queryPanel.add(queryTextArea);

        JPanel inputsPanel = new JPanel();
        inputsPanel.setLayout(new GridLayout(1, 2, 20, 20));
        inputsPanel.setPreferredSize(new Dimension(width, 200));
        inputsPanel.add(ddlPanel);
        inputsPanel.add(queryPanel);
        pane.add(inputsPanel);

        JButton convertButton = new JButton("Convert");
        JButton copyButton = new JButton("Copy Output");

        JPanel buttonContainer = new JPanel();
        buttonContainer.setLayout(new GridLayout(1, 2, 20, 0));
        addPadding(buttonContainer, 10, 0, 10, 0);
        buttonContainer.add(convertButton);
        buttonContainer.add(copyButton);
        buttonContainer.setMaximumSize(new Dimension(width, 20));
        pane.add(buttonContainer);

        JLabel outputLabel = new JLabel("C Code Output");
        setLabelStyle(outputLabel);
        outputLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
        JTextArea codeOutputTextArea = new JTextArea();
        codeOutputTextArea.setLineWrap(true);
        codeOutputTextArea.setAutoscrolls(true);
        setComponentFont(codeOutputTextArea);
        addPadding(codeOutputTextArea, 5, 5, 5, 5);
        codeOutputTextArea.setEditable(false);
        JScrollPane scrollableOutput = new JScrollPane(codeOutputTextArea, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        scrollableOutput.setPreferredSize(new Dimension(width, 300));

        JPanel outputPanel = new JPanel();
        outputPanel.setLayout(new BoxLayout(outputPanel, BoxLayout.Y_AXIS));
        addPadding(outputLabel, 0, 0, 8, 0);
        outputPanel.add(outputLabel);
        outputPanel.add(scrollableOutput);

        pane.add(outputPanel);

        convertButton.addActionListener(e -> {
            // Get and cleanup ddl
            String[] ddlInput = ddlTextArea.getText().split(";");
            ArrayList<String> ddl = new ArrayList<>();
            for (String s : ddlInput) {
                String strippedDDL = s.strip();
                if (!strippedDDL.isEmpty()) ddl.add(strippedDDL);
            }

            // Get and cleanup query
            String query = queryTextArea.getText();
            if (query.split(";").length > 1) {
                setIsError(codeOutputTextArea, true);
                codeOutputTextArea.setText("ERROR: Please only enter one query at a time");
                return;
            }

            try (SQLConverter sqlConverter = new SQLConverter(ddl.toArray(new String[0]))) {
                String cCode = sqlConverter.toCCode(query);
                setIsError(codeOutputTextArea, false);
                codeOutputTextArea.setText(cCode);
            } catch (Exception ex) {
                setIsError(codeOutputTextArea, true);
                codeOutputTextArea.setText("ERROR: " + ex.getMessage());
            }
        });

        copyButton.addActionListener(e -> {
            String output = codeOutputTextArea.getText().strip();
            String msg;
            if (!output.isEmpty()) {
                StringSelection stringSelection = new StringSelection(output);
                Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
                clipboard.setContents(stringSelection, null);
                msg = "Copied!";
            } else {
                msg = "Nothing to copy!";
            }

            // Display msg
            String oldText = copyButton.getText();
            copyButton.setText(msg);
            copyButton.setEnabled(false);
            SwingUtilities.invokeLater(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                } finally {
                    copyButton.setText(oldText);
                    copyButton.setEnabled(true);
                }
            });
        });

        addPadding(frame.getRootPane(), 20, 20, 20, 20);
        frame.pack();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }

    private static void setLabelStyle(JLabel label) {
        label.setFont(new Font("Arial", Font.BOLD, 14));
    }

    private static void setComponentFont(JComponent component) {
        component.setFont(new Font("Consolas", Font.PLAIN, 12));
    }

    private static void addPadding(JComponent component, int top, int left, int bottom, int right) {
        component.setBorder(BorderFactory.createCompoundBorder(component.getBorder(), BorderFactory.createEmptyBorder(top, left, bottom, right)));
    }

    private static void setIsError(JTextArea textArea, boolean isError) {
        if (isError) {
            textArea.setForeground(Color.RED);
        } else {
            textArea.setForeground(Color.BLACK);
        }
    }
}
