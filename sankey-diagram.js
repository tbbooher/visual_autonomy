document.addEventListener('DOMContentLoaded', function() {
    try {
        if (typeof d3 === 'undefined') throw new Error('D3 library is not loaded');
        if (typeof d3.sankey === 'undefined') throw new Error('D3 Sankey plugin is not loaded');

        // Load data from flow_data.json
        d3.json('flow_data.json').then(function(data) {

            const margin = {top: 10, right: 10, bottom: 10, left: 10};
            const width = 900 - margin.left - margin.right;
            const height = 600 - margin.top - margin.bottom;

            const svg = d3.select("#chart").append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", `translate(${margin.left},${margin.top})`);

            // Create a set of all unique node names
            const nodeNames = new Set();
            data.forEach(d => {
                nodeNames.add(d.source);
                nodeNames.add(d.target);
            });

            // Create an array of node objects with funding information
            const nodes = Array.from(nodeNames).map(name => ({
                name: name,
                fundingIn: 0,
                fundingOut: 0
            }));

            // Calculate total funding for each node
            data.forEach(d => {
                const sourceNode = nodes.find(n => n.name === d.source);
                const targetNode = nodes.find(n => n.name === d.target);
                sourceNode.fundingOut += d.value;
                targetNode.fundingIn += d.value;
            });

            // Create a map of node names to indices
            const nodeMap = new Map(nodes.map((node, index) => [node.name, index]));

            // Map the links to use node indices instead of names
            const links = data.map(d => ({
                source: nodeMap.get(d.source),
                target: nodeMap.get(d.target),
                value: d.value,
                source_funding: d.source_funding,
                target_funding: d.target_funding
            }));

            const sankey = d3.sankey()
                .nodeWidth(30)
                .nodePadding(10)
                .extent([[1, 1], [width - 1, height - 6]])
                .nodeId(d => d.index);

            const graph = sankey({
                nodes: nodes,
                links: links
            });

            // Color scale for nodes
            const color = d3.scaleOrdinal(d3.schemeCategory10);

            // Function to update the diagram
            function updateDiagram() {
                sankey.update(graph);
                link.attr("d", d3.sankeyLinkHorizontal());
                node.attr("transform", d => `translate(${d.x0},${d.y0})`);
                nodeRect.attr("height", d => d.y1 - d.y0);
                nodeOutRect.attr("height", d => (d.fundingOut / (d.value || 1)) * (d.y1 - d.y0));
                nodeText.attr("y", d => (d.y1 - d.y0) / 2);
            }

            // Drag behavior
            const drag = d3.drag()
                .subject(function(event, d) {
                    return d;
                })
                .on("start", function(event, d) {
                    d3.select(this).raise().attr("cursor", "grabbing");
                })
                .on("drag", function(event, d) {
                    const dy = event.dy;
                    d.y0 = Math.max(0, Math.min(height - (d.y1 - d.y0), d.y0 + dy));
                    d.y1 = d.y0 + (d.y1 - d.y0);
                    updateDiagram();
                })
                .on("end", function(event, d) {
                    d3.select(this).attr("cursor", "grab");
                });

            const link = svg.append("g")
                .selectAll(".link")
                .data(graph.links)
                .enter().append("path")
                .attr("class", "link")
                .attr("d", d3.sankeyLinkHorizontal())
                .attr("stroke-width", d => Math.max(1, d.width))
                .attr("stroke", d => color(d.source.name))
                .attr("stroke-opacity", 0.5)
                .attr("fill", "none");

            const node = svg.append("g")
                .selectAll(".node")
                .data(graph.nodes)
                .enter().append("g")
                .attr("class", "node")
                .attr("transform", d => `translate(${d.x0},${d.y0})`)
                .attr("cursor", "grab")
                .call(drag);

            // Create two rectangles for each node
            const nodeRect = node.append("rect")
                .attr("x", -sankey.nodeWidth())
                .attr("width", sankey.nodeWidth())
                .attr("fill", d => color(d.name))
                .attr("opacity", 0.8);

            const nodeOutRect = node.append("rect")
                .attr("x", -sankey.nodeWidth() / 2)
                .attr("width", sankey.nodeWidth() / 2)
                .attr("fill", d => d3.color(color(d.name)).brighter(0.5))
                .attr("opacity", 0.8);

            const nodeText = node.append("text")
                .attr("x", d => d.x0 < width / 2 ? 6 : -6)
                .attr("dy", "0.35em")
                .attr("text-anchor", d => d.x0 < width / 2 ? "start" : "end")
                .text(d => `${d.name} (In: $${d.fundingIn.toFixed(2)}M, Out: $${d.fundingOut.toFixed(2)}M)`)
                .attr("fill", "black")
                .attr("font-size", "10px");

            node.append("title")
                .text(d => `${d.name}\nTotal Funding In: $${d.fundingIn.toFixed(2)}M\nTotal Funding Out: $${d.fundingOut.toFixed(2)}M`);

            link.append("title")
                .text(d => `${d.source.name} → ${d.target.name}\nValue: $${d.value.toFixed(2)}M\nSource Funding: $${d.source_funding.toFixed(2)}M\nTarget Funding: $${d.target_funding.toFixed(2)}M`);

        }).catch(function(error) {
            console.error('Error loading JSON data:', error);
            document.getElementById('error').textContent = 'Error loading data: ' + error.message;
        });

    } catch (error) {
        console.error('Error:', error);
        document.getElementById('error').textContent = 'Error: ' + error.message;
    }
});