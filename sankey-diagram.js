document.addEventListener("DOMContentLoaded", function () {
    console.log("DOM loaded");
    try {
        if (typeof d3 === "undefined") throw new Error("D3 library is not loaded");
        if (typeof d3.sankey === "undefined")
            throw new Error("D3 Sankey plugin is not loaded");

        // Load data from an external JSON file
        d3.json("flow_data.json").then(function (data) {
            const margin = { top: 10, right: 10, bottom: 10, left: 10 };
            const width = 900 - margin.left - margin.right;
            const height = 600 - margin.top - margin.bottom;

            // Group data by unique source themes
            const themes = Array.from(new Set(data.map(d => d.source_theme)));

            themes.forEach(theme => {
                // Filter data for the current theme
                const themeData = data.filter(d => d.source_theme === theme);

                // Create a container for each Sankey diagram
                const container = d3.select("#chart").append("div").attr("class", "theme-container");

                // Add a title for the theme
                container.append("h2").text(`Sankey Diagram for Theme: ${theme}`);

                // Create an SVG for the Sankey diagram
                const svg = container.append("svg")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", height + margin.top + margin.bottom)
                    .append("g")
                    .attr("transform", `translate(${margin.left},${margin.top})`);

                // Create a color scale for the nodes
                const color = d3.scaleOrdinal(d3.schemeCategory10);

                // Create a set of all unique node names for the current theme
                const nodeNames = new Set();
                themeData.forEach((d) => {
                    nodeNames.add(d.source);
                    nodeNames.add(d.target);
                });

                // Create an array of node objects with funding information
                const nodes = Array.from(nodeNames).map((name) => ({
                    name: name,
                    fundingIn: 0,
                    fundingOut: 0,
                    targetFunding: 0 // Add targetFunding to handle final node case
                }));

                // Calculate total funding for each node
                themeData.forEach((d) => {
                    const sourceNode = nodes.find((n) => n.name === d.source);
                    const targetNode = nodes.find((n) => n.name === d.target);
                    if (sourceNode) sourceNode.fundingOut += d.value;
                    if (targetNode) {
                        targetNode.fundingIn += d.value;
                        targetNode.targetFunding = d.target_funding; // Assign targetFunding for final node
                    }
                });

                // Create a map of node names to indices
                const nodeMap = new Map(nodes.map((node, index) => [node.name, index]));

                // Map the links to use node indices instead of names
                const links = themeData.map((d) => ({
                    source: nodeMap.get(d.source),
                    target: nodeMap.get(d.target),
                    value: d.value,
                    source_funding: d.source_funding || 0,
                    target_funding: d.target_funding || 0
                }));

                const sankey = d3.sankey()
                    .nodeWidth(30)
                    .nodePadding(10)
                    .extent([
                        [1, 1],
                        [width - 1, height - 6]
                    ])
                    .nodeId((d) => d.index);

                const graph = sankey({
                    nodes: nodes.map((d) => Object.assign({}, d)), // Ensure we pass a copy
                    links: links.map((d) => Object.assign({}, d)) // Ensure we pass a copy
                });

                const link = svg.append("g")
                    .selectAll(".link")
                    .data(graph.links)
                    .enter()
                    .append("path")
                    .attr("class", "link")
                    .attr("d", d3.sankeyLinkHorizontal())
                    .style("stroke-width", (d) => Math.max(1, d.width));

                const node = svg.append("g")
                    .selectAll(".node")
                    .data(graph.nodes)
                    .enter()
                    .append("g")
                    .attr("class", "node")
                    .attr("transform", (d) => `translate(${d.x0},${d.y0})`)
                    .call(
                        d3.drag()
                            .subject(function (d) {
                                return d;
                            })
                            .on("start", function () {
                                this.parentNode.appendChild(this);
                            })
                            .on("drag", dragmove)
                    );

                // Create the in-bar with adjusted height
                node.append("rect")
                    .attr("class", "in-bar")
                    .attr("x", 0)
                    .attr("height", (d) => {
                        const baseHeight = d.y1 - d.y0;
                        if (d.fundingOut > d.fundingIn) {
                            return (d.fundingIn / d.fundingOut) * baseHeight;
                        }
                        return baseHeight;
                    })
                    .attr("width", sankey.nodeWidth() / 2)
                    .attr("fill", (d) => color(d.name))
                    .attr("opacity", 0.7);

                // Create the out-bar or target-funding bar for final nodes
                node.append("rect")
                    .attr("class", "out-bar")
                    .attr("x", sankey.nodeWidth() / 2)
                    .attr("height", (d) => {
                        const baseHeight = d.y1 - d.y0;
                        const isFinalNode = graph.links.every(link => link.source.index !== d.index);
                        if (isFinalNode) {
                            // return (d.fundingOut / d.fundingIn) * baseHeight;
                            return baseHeight*2;
                        } else if (d.fundingOut > d.fundingIn) {
                            return baseHeight;
                        } else {
                            return (d.fundingOut / d.fundingIn) * baseHeight;
                        }
                    })
                    .attr("width", sankey.nodeWidth() / 2)
                    .attr("fill", (d) => d3.color(color(d.name)).darker(0.5))
                    .attr("opacity", 0.7);

                node.append("text")
                    .attr("x", (d) => (d.x0 < width / 2 ? 6 + sankey.nodeWidth() : -6))
                    .attr("y", (d) => (d.y1 - d.y0) / 2)
                    .attr("dy", "0.35em")
                    .attr("text-anchor", (d) => (d.x0 < width / 2 ? "start" : "end"))
                    .text((d) => {
                        const isFinalNode = graph.links.every(link => link.source.index !== d.index);
                        if (isFinalNode) {
                            return `${d.name} (${d.targ})`;
                        } else {
                            return `${d.name} (${d.fundingOut})`;
                        }
                    });

                node.append("title")
                    .text(d => `${d.name}\nTotal Funding In: $${d.fundingIn.toFixed(2)}M\nTotal Funding Out: $${d.fundingOut.toFixed(2)}M\nTarget Funding: $${d.targetFunding.toFixed(2)}M`);

                link.append("title")
                    .text(d => `${d.source.name} → ${d.target.name}\nValue: $${d.value.toFixed(2)}M\nSource Funding: $${d.source_funding.toFixed(2)}M\nTarget Funding: $${d.target_funding.toFixed(2)}M`);

                // The function for moving the nodes
                function dragmove(d) {
                    d3.select(this).attr(
                        "transform",
                        "translate(" +
                        d.x0 +
                        "," +
                        (d.y0 = Math.max(0, Math.min(height - (d.y1 - d.y0), d3.event.y))) +
                        ")"
                    );
                    sankey.update(graph);
                    link.attr("d", d3.sankeyLinkHorizontal());
                }
            });

        }).catch(function (error) {
            console.error("Error loading data:", error);
            document.getElementById("error").textContent = "Error loading data: " + error.message;
        });

    } catch (error) {
        console.error("Error:", error);
        document.getElementById("error").textContent = "Error: " + error.message;
    }
});
