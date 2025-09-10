#!/usr/bin/env python3
"""
Results viewer for benchmark results
"""

import os
import json
import argparse
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import matplotlib.pyplot as plt
import seaborn as sns


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="View benchmark results")
    parser.add_argument('result_file', help='Path to result JSON file')
    parser.add_argument('--format', choices=['table', 'plot', 'both'], default='both',
                       help='Output format')
    parser.add_argument('--output', help='Output file for plots (optional)')
    
    args = parser.parse_args()
    
    # Load results
    with open(args.result_file, 'r') as f:
        results = json.load(f)
    
    # Create tables
    if args.format in ['table', 'both']:
        print_tables(results)
    
    # Create plots
    if args.format in ['plot', 'both']:
        create_plots(results, args.output)


def print_tables(results: Dict[str, Any]):
    """Print results in table format"""
    
    print("\n" + "="*80)
    print("BENCHMARK RESULTS SUMMARY")
    print("="*80)
    
    metadata = results.get('metadata', {})
    print(f"Timestamp: {metadata.get('timestamp')}")
    print(f"Engines: {metadata.get('engines')}")
    print(f"Iterations: {metadata.get('iterations')}")
    print(f"Queries: {len(metadata.get('queries', []))}")
    
    # Create summary table
    summary_data = []
    
    for query_name, query_data in results.get('queries', {}).items():
        query_info = query_data.get('query', {})
        
        for engine_name, engine_data in query_data.get('engines', {}).items():
            stats = engine_data.get('statistics', {})
            
            summary_data.append({
                'Query': query_name,
                'Query Name': query_info.get('name', ''),
                'Engine': engine_name,
                'Avg Time (s)': f"{stats.get('avg_time', 0):.3f}" if stats.get('avg_time') else 'FAILED',
                'Min Time (s)': f"{stats.get('min_time', 0):.3f}" if stats.get('min_time') else 'FAILED',
                'Max Time (s)': f"{stats.get('max_time', 0):.3f}" if stats.get('max_time') else 'FAILED',
                'Success Rate': f"{stats.get('success_rate', 0):.1%}",
                'Successful Runs': engine_data.get('success_count', 0),
                'Failed Runs': engine_data.get('error_count', 0)
            })
    
    if summary_data:
        df = pd.DataFrame(summary_data)
        print("\n" + "="*120)
        print("DETAILED RESULTS")
        print("="*120)
        print(df.to_string(index=False))
        
        # Performance comparison
        print("\n" + "="*80)
        print("PERFORMANCE COMPARISON (Average Times)")
        print("="*80)
        
        perf_data = []
        for query_name in df['Query'].unique():
            query_df = df[df['Query'] == query_name]
            for _, row in query_df.iterrows():
                try:
                    avg_time = float(row['Avg Time (s)'])
                    perf_data.append({
                        'Query': query_name,
                        'Engine': row['Engine'],
                        'Avg Time': avg_time
                    })
                except:
                    pass
        
        if perf_data:
            perf_df = pd.DataFrame(perf_data)
            pivot_df = perf_df.pivot(index='Query', columns='Engine', values='Avg Time')
            print(pivot_df.round(3))
            
            # Speedup analysis
            print("\n" + "="*80)
            print("SPEEDUP ANALYSIS (Relative to slowest)")
            print("="*80)
            
            speedup_df = pivot_df.div(pivot_df.max(axis=1), axis=0)
            speedup_df = speedup_df.round(3)
            print(speedup_df)


def create_plots(results: Dict[str, Any], output_file: str = None):
    """Create visualization plots"""
    
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # Set style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Prepare data
        plot_data = []
        
        for query_name, query_data in results.get('queries', {}).items():
            for engine_name, engine_data in query_data.get('engines', {}).items():
                stats = engine_data.get('statistics', {})
                avg_time = stats.get('avg_time')
                
                if avg_time is not None:
                    plot_data.append({
                        'Query': query_name,
                        'Engine': engine_name,
                        'Avg Time (s)': avg_time,
                        'Success Rate': stats.get('success_rate', 0)
                    })
        
        if not plot_data:
            print("No data available for plotting")
            return
            
        df = pd.DataFrame(plot_data)
        
        # Create subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Lakehouse Engines Benchmark Results', fontsize=16, fontweight='bold')
        
        # 1. Average execution times by query and engine
        pivot_df = df.pivot(index='Query', columns='Engine', values='Avg Time (s)')
        pivot_df.plot(kind='bar', ax=ax1)
        ax1.set_title('Average Execution Time by Query')
        ax1.set_ylabel('Time (seconds)')
        ax1.tick_params(axis='x', rotation=45)
        ax1.legend(title='Engine')
        
        # 2. Success rates
        success_pivot = df.pivot(index='Query', columns='Engine', values='Success Rate')
        success_pivot.plot(kind='bar', ax=ax2)
        ax2.set_title('Success Rate by Query')
        ax2.set_ylabel('Success Rate')
        ax2.tick_params(axis='x', rotation=45)
        ax2.legend(title='Engine')
        ax2.set_ylim(0, 1.1)
        
        # 3. Performance comparison (heatmap)
        sns.heatmap(pivot_df, annot=True, fmt='.3f', cmap='RdYlGn_r', ax=ax3)
        ax3.set_title('Performance Heatmap (seconds)')
        ax3.set_xlabel('Engine')
        ax3.set_ylabel('Query')
        
        # 4. Overall engine performance
        engine_avg = df.groupby('Engine')['Avg Time (s)'].mean().sort_values()
        engine_avg.plot(kind='bar', ax=ax4, color=['#1f77b4', '#ff7f0e', '#2ca02c'])
        ax4.set_title('Overall Average Performance by Engine')
        ax4.set_ylabel('Average Time (seconds)')
        ax4.tick_params(axis='x', rotation=0)
        
        plt.tight_layout()
        
        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"Plot saved to: {output_file}")
        else:
            plt.show()
            
    except ImportError:
        print("Matplotlib/Seaborn not available. Install with:")
        print("pip install matplotlib seaborn")


if __name__ == '__main__':
    main()
