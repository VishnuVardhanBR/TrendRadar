# coding=utf-8
"""
TrendRadar Main Program

Trending news aggregation and analysis tool
Usage: python -m trendradar
"""

import os
import webbrowser
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests

from trendradar.context import AppContext
from trendradar import __version__
from trendradar.core import load_config
from trendradar.crawler import DataFetcher
from trendradar.storage import convert_crawl_results_to_news_data


def check_version_update(
    current_version: str, version_url: str, proxy_url: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """Check for version updates"""
    try:
        proxies = None
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/plain, */*",
            "Cache-Control": "no-cache",
        }

        response = requests.get(
            version_url, proxies=proxies, headers=headers, timeout=10
        )
        response.raise_for_status()

        remote_version = response.text.strip()
        print(f"Current version: {current_version}, Remote version: {remote_version}")

        # Compare versions
        def parse_version(version_str):
            try:
                parts = version_str.strip().split(".")
                if len(parts) != 3:
                    raise ValueError("Invalid version format")
                return int(parts[0]), int(parts[1]), int(parts[2])
            except:
                return 0, 0, 0

        current_tuple = parse_version(current_version)
        remote_tuple = parse_version(remote_version)

        need_update = current_tuple < remote_tuple
        return need_update, remote_version if need_update else None

    except Exception as e:
        print(f"Version check failed: {e}")
        return False, None


# === Main Analyzer ===
class NewsAnalyzer:
    """News Analyzer"""

    # Mode strategy definitions
    MODE_STRATEGIES = {
        "incremental": {
            "mode_name": "Incremental Mode",
            "description": "Incremental mode (only new news, no push if nothing new)",
            "realtime_report_type": "Real-time Incremental",
            "summary_report_type": "Daily Summary",
            "should_send_realtime": True,
            "should_generate_summary": True,
            "summary_mode": "daily",
        },
        "current": {
            "mode_name": "Current Rankings Mode",
            "description": "Current rankings mode (current matches + new news section + scheduled push)",
            "realtime_report_type": "Real-time Current Rankings",
            "summary_report_type": "Current Rankings Summary",
            "should_send_realtime": True,
            "should_generate_summary": True,
            "summary_mode": "current",
        },
        "daily": {
            "mode_name": "Daily Summary Mode",
            "description": "Daily summary mode (all matches + new news section + scheduled push)",
            "realtime_report_type": "",
            "summary_report_type": "Daily Summary",
            "should_send_realtime": False,
            "should_generate_summary": True,
            "summary_mode": "daily",
        },
    }

    def __init__(self):
        # Load configuration
        print("Loading configuration...")
        config = load_config()
        print(f"TrendRadar v{__version__} configuration loaded")
        print(f"Monitored platforms: {len(config['PLATFORMS'])}")
        print(f"Timezone: {config.get('TIMEZONE', 'America/New_York')}")

        # 创建应用上下文
        self.ctx = AppContext(config)

        self.request_interval = self.ctx.config["REQUEST_INTERVAL"]
        self.report_mode = self.ctx.config["REPORT_MODE"]
        self.rank_threshold = self.ctx.rank_threshold
        self.is_github_actions = os.environ.get("GITHUB_ACTIONS") == "true"
        self.is_docker_container = self._detect_docker_environment()
        self.update_info = None
        self.proxy_url = None
        self._setup_proxy()
        self.data_fetcher = DataFetcher(self.proxy_url)

        # 初始化存储管理器（使用 AppContext）
        self._init_storage_manager()

        if self.is_github_actions:
            self._check_version_update()

    def _init_storage_manager(self) -> None:
        """Initialize storage manager (using AppContext)"""
        # Get data retention days (supports environment variable override)
        env_retention = os.environ.get("STORAGE_RETENTION_DAYS", "").strip()
        if env_retention:
            # Environment variable overrides config
            self.ctx.config["STORAGE"]["RETENTION_DAYS"] = int(env_retention)

        self.storage_manager = self.ctx.get_storage_manager()
        print(f"Storage backend: {self.storage_manager.backend_name}")

        retention_days = self.ctx.config.get("STORAGE", {}).get("RETENTION_DAYS", 0)
        if retention_days > 0:
            print(f"Data retention: {retention_days} days")

    def _detect_docker_environment(self) -> bool:
        """Detect if running in Docker container"""
        try:
            if os.environ.get("DOCKER_CONTAINER") == "true":
                return True

            if os.path.exists("/.dockerenv"):
                return True

            return False
        except Exception:
            return False

    def _should_open_browser(self) -> bool:
        """Determine if browser should be opened"""
        return not self.is_github_actions and not self.is_docker_container

    def _setup_proxy(self) -> None:
        """Set up proxy configuration"""
        if not self.is_github_actions and self.ctx.config["USE_PROXY"]:
            self.proxy_url = self.ctx.config["DEFAULT_PROXY"]
            print("Local environment, using proxy")
        elif not self.is_github_actions and not self.ctx.config["USE_PROXY"]:
            print("Local environment, proxy disabled")
        else:
            print("GitHub Actions environment, proxy disabled")

    def _check_version_update(self) -> None:
        """Check for version updates"""
        try:
            need_update, remote_version = check_version_update(
                __version__, self.ctx.config["VERSION_CHECK_URL"], self.proxy_url
            )

            if need_update and remote_version:
                self.update_info = {
                    "current_version": __version__,
                    "remote_version": remote_version,
                }
                print(f"New version available: {remote_version} (current: {__version__})")
            else:
                print("Version check complete, you have the latest version")
        except Exception as e:
            print(f"Version check error: {e}")

    def _get_mode_strategy(self) -> Dict:
        """Get current mode strategy configuration"""
        return self.MODE_STRATEGIES.get(self.report_mode, self.MODE_STRATEGIES["daily"])

    def _has_notification_configured(self) -> bool:
        """Check if any notification channel is configured"""
        cfg = self.ctx.config
        return any(
            [
                cfg["FEISHU_WEBHOOK_URL"],
                cfg["DINGTALK_WEBHOOK_URL"],
                cfg["WEWORK_WEBHOOK_URL"],
                (cfg["TELEGRAM_BOT_TOKEN"] and cfg["TELEGRAM_CHAT_ID"]),
                (
                    cfg["EMAIL_FROM"]
                    and cfg["EMAIL_PASSWORD"]
                    and cfg["EMAIL_TO"]
                ),
                (cfg["NTFY_SERVER_URL"] and cfg["NTFY_TOPIC"]),
                cfg["BARK_URL"],
                cfg["SLACK_WEBHOOK_URL"],
            ]
        )

    def _has_valid_content(
        self, stats: List[Dict], new_titles: Optional[Dict] = None
    ) -> bool:
        """Check if there is valid news content"""
        if self.report_mode == "incremental":
            # Incremental mode: must have new titles matching keywords
            has_new_titles = bool(
                new_titles and any(len(titles) > 0 for titles in new_titles.values())
            )
            has_matched_news = any(stat["count"] > 0 for stat in stats)
            return has_new_titles and has_matched_news
        elif self.report_mode == "current":
            # Current mode: push if stats has any matched content
            return any(stat["count"] > 0 for stat in stats)
        else:
            # Daily summary mode: check for matched keywords or new news
            has_matched_news = any(stat["count"] > 0 for stat in stats)
            has_new_news = bool(
                new_titles and any(len(titles) > 0 for titles in new_titles.values())
            )
            return has_matched_news or has_new_news

    def _load_analysis_data(
        self,
        quiet: bool = False,
    ) -> Optional[Tuple[Dict, Dict, Dict, Dict, List, List]]:
        """Unified data loading and preprocessing, filtered by current platform list"""
        try:
            # Get current configured platform IDs
            current_platform_ids = self.ctx.platform_ids
            if not quiet:
                print(f"Current platforms: {current_platform_ids}")

            all_results, id_to_name, title_info = self.ctx.read_today_titles(
                current_platform_ids, quiet=quiet
            )

            if not all_results:
                print("No data found for today")
                return None

            total_titles = sum(len(titles) for titles in all_results.values())
            if not quiet:
                print(f"Loaded {total_titles} titles (filtered by current platforms)")

            new_titles = self.ctx.detect_new_titles(current_platform_ids, quiet=quiet)
            word_groups, filter_words, global_filters = self.ctx.load_frequency_words()

            return (
                all_results,
                id_to_name,
                title_info,
                new_titles,
                word_groups,
                filter_words,
                global_filters,
            )
        except Exception as e:
            print(f"Data loading failed: {e}")
            return None

    def _prepare_current_title_info(self, results: Dict, time_info: str) -> Dict:
        """Build title info from current crawl results"""
        title_info = {}
        for source_id, titles_data in results.items():
            title_info[source_id] = {}
            for title, title_data in titles_data.items():
                ranks = title_data.get("ranks", [])
                url = title_data.get("url", "")
                mobile_url = title_data.get("mobileUrl", "")

                title_info[source_id][title] = {
                    "first_time": time_info,
                    "last_time": time_info,
                    "count": 1,
                    "ranks": ranks,
                    "url": url,
                    "mobileUrl": mobile_url,
                }
        return title_info

    def _run_analysis_pipeline(
        self,
        data_source: Dict,
        mode: str,
        title_info: Dict,
        new_titles: Dict,
        word_groups: List[Dict],
        filter_words: List[str],
        id_to_name: Dict,
        failed_ids: Optional[List] = None,
        is_daily_summary: bool = False,
        global_filters: Optional[List[str]] = None,
        quiet: bool = False,
    ) -> Tuple[List[Dict], Optional[str]]:
        """Unified analysis pipeline: data processing → statistics → HTML generation"""

        # Calculate statistics (using AppContext)
        stats, total_titles = self.ctx.count_frequency(
            data_source,
            word_groups,
            filter_words,
            id_to_name,
            title_info,
            new_titles,
            mode=mode,
            global_filters=global_filters,
            quiet=quiet,
        )

        # Generate HTML (if enabled)
        html_file = None
        if self.ctx.config["STORAGE"]["FORMATS"]["HTML"]:
            html_file = self.ctx.generate_html(
                stats,
                total_titles,
                failed_ids=failed_ids,
                new_titles=new_titles,
                id_to_name=id_to_name,
                mode=mode,
                is_daily_summary=is_daily_summary,
                update_info=self.update_info if self.ctx.config["SHOW_VERSION_UPDATE"] else None,
            )

        return stats, html_file

    def _send_notification_if_needed(
        self,
        stats: List[Dict],
        report_type: str,
        mode: str,
        failed_ids: Optional[List] = None,
        new_titles: Optional[Dict] = None,
        id_to_name: Optional[Dict] = None,
        html_file_path: Optional[str] = None,
    ) -> bool:
        """Unified notification sending logic with all conditions"""
        has_notification = self._has_notification_configured()
        cfg = self.ctx.config

        if (
            cfg["ENABLE_NOTIFICATION"]
            and has_notification
            and self._has_valid_content(stats, new_titles)
        ):
            # Push window control
            if cfg["PUSH_WINDOW"]["ENABLED"]:
                push_manager = self.ctx.create_push_manager()
                time_range_start = cfg["PUSH_WINDOW"]["TIME_RANGE"]["START"]
                time_range_end = cfg["PUSH_WINDOW"]["TIME_RANGE"]["END"]

                if not push_manager.is_in_time_range(time_range_start, time_range_end):
                    now = self.ctx.get_time()
                    print(
                        f"Push window control: Current time {now.strftime('%H:%M')} is outside push window {time_range_start}-{time_range_end}, skipping"
                    )
                    return False

                if cfg["PUSH_WINDOW"]["ONCE_PER_DAY"]:
                    if push_manager.has_pushed_today():
                        print(f"Push window control: Already pushed today, skipping")
                        return False
                    else:
                        print(f"Push window control: First push today")

            # Prepare report data
            report_data = self.ctx.prepare_report(stats, failed_ids, new_titles, id_to_name, mode)

            # Whether to send version update info
            update_info_to_send = self.update_info if cfg["SHOW_VERSION_UPDATE"] else None

            # Use NotificationDispatcher to send to all channels
            dispatcher = self.ctx.create_notification_dispatcher()
            results = dispatcher.dispatch_all(
                report_data=report_data,
                report_type=report_type,
                update_info=update_info_to_send,
                proxy_url=self.proxy_url,
                mode=mode,
                html_file_path=html_file_path,
            )

            if not results:
                print("No notification channels configured, skipping")
                return False

            # Record push if successful and once_per_day is enabled
            if (
                cfg["PUSH_WINDOW"]["ENABLED"]
                and cfg["PUSH_WINDOW"]["ONCE_PER_DAY"]
                and any(results.values())
            ):
                push_manager = self.ctx.create_push_manager()
                push_manager.record_push(report_type)

            return True

        elif cfg["ENABLE_NOTIFICATION"] and not has_notification:
            print("⚠️ Warning: Notifications enabled but no channels configured, skipping")
        elif not cfg["ENABLE_NOTIFICATION"]:
            print(f"Skipping {report_type} notification: Notifications disabled")
        elif (
            cfg["ENABLE_NOTIFICATION"]
            and has_notification
            and not self._has_valid_content(stats, new_titles)
        ):
            mode_strategy = self._get_mode_strategy()
            if "Real-time" in report_type or "实时" in report_type:
                if self.report_mode == "incremental":
                    has_new = bool(
                        new_titles and any(len(titles) > 0 for titles in new_titles.values())
                    )
                    if not has_new:
                        print("Skipping real-time notification: No new news detected in incremental mode")
                    else:
                        print("Skipping real-time notification: New news didn't match any keywords")
                else:
                    print(
                        f"Skipping real-time notification: No matched news in {mode_strategy['mode_name']}"
                    )
            else:
                print(
                    f"Skipping {mode_strategy['summary_report_type']} notification: No valid news content"
                )

        return False

    def _generate_summary_report(self, mode_strategy: Dict) -> Optional[str]:
        """Generate summary report (with notification)"""
        summary_type = (
            "Current Rankings Summary" if mode_strategy["summary_mode"] == "current" else "Daily Summary"
        )
        print(f"Generating {summary_type} report...")

        # Load analysis data
        analysis_data = self._load_analysis_data()
        if not analysis_data:
            return None

        all_results, id_to_name, title_info, new_titles, word_groups, filter_words, global_filters = (
            analysis_data
        )

        # Run analysis pipeline
        stats, html_file = self._run_analysis_pipeline(
            all_results,
            mode_strategy["summary_mode"],
            title_info,
            new_titles,
            word_groups,
            filter_words,
            id_to_name,
            is_daily_summary=True,
            global_filters=global_filters,
        )

        if html_file:
            print(f"{summary_type} report generated: {html_file}")

        # Send notification
        self._send_notification_if_needed(
            stats,
            mode_strategy["summary_report_type"],
            mode_strategy["summary_mode"],
            failed_ids=[],
            new_titles=new_titles,
            id_to_name=id_to_name,
            html_file_path=html_file,
        )

        return html_file

    def _generate_summary_html(self, mode: str = "daily") -> Optional[str]:
        """Generate summary HTML"""
        summary_type = "Current Rankings Summary" if mode == "current" else "Daily Summary"
        print(f"Generating {summary_type} HTML...")

        # Load analysis data (quiet mode to avoid duplicate logs)
        analysis_data = self._load_analysis_data(quiet=True)
        if not analysis_data:
            return None

        all_results, id_to_name, title_info, new_titles, word_groups, filter_words, global_filters = (
            analysis_data
        )

        # Run analysis pipeline (quiet mode)
        _, html_file = self._run_analysis_pipeline(
            all_results,
            mode,
            title_info,
            new_titles,
            word_groups,
            filter_words,
            id_to_name,
            is_daily_summary=True,
            global_filters=global_filters,
            quiet=True,
        )

        if html_file:
            print(f"{summary_type} HTML generated: {html_file}")
        return html_file

    def _initialize_and_check_config(self) -> None:
        """Common initialization and config check"""
        now = self.ctx.get_time()
        print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

        if not self.ctx.config["ENABLE_CRAWLER"]:
            print("Crawler disabled (ENABLE_CRAWLER=False), exiting")
            return

        has_notification = self._has_notification_configured()
        if not self.ctx.config["ENABLE_NOTIFICATION"]:
            print("Notifications disabled (ENABLE_NOTIFICATION=False), data crawling only")
        elif not has_notification:
            print("No notification channels configured, data crawling only")
        else:
            print("Notifications enabled, will send alerts")

        mode_strategy = self._get_mode_strategy()
        print(f"Report mode: {self.report_mode}")
        print(f"Run mode: {mode_strategy['description']}")

    def _crawl_data(self) -> Tuple[Dict, Dict, List]:
        """Execute data crawling"""
        ids = []
        for platform in self.ctx.platforms:
            if "name" in platform:
                ids.append((platform["id"], platform["name"]))
            else:
                ids.append(platform["id"])

        print(
            f"Configured platforms: {[p.get('name', p['id']) for p in self.ctx.platforms]}"
        )
        print(f"Starting data crawl, request interval {self.request_interval}ms")
        Path("output").mkdir(parents=True, exist_ok=True)

        results, id_to_name, failed_ids = self.data_fetcher.crawl_websites(
            ids, self.request_interval
        )

        # Convert to NewsData format and save to storage backend
        crawl_time = self.ctx.format_time()
        crawl_date = self.ctx.format_date()
        news_data = convert_crawl_results_to_news_data(
            results, id_to_name, failed_ids, crawl_time, crawl_date
        )

        # Save to storage backend (SQLite)
        if self.storage_manager.save_news_data(news_data):
            print(f"Data saved to storage backend: {self.storage_manager.backend_name}")

        # Save TXT snapshot (if enabled)
        txt_file = self.storage_manager.save_txt_snapshot(news_data)
        if txt_file:
            print(f"TXT snapshot saved: {txt_file}")

        # Compatibility: also save to original TXT format
        if self.ctx.config["STORAGE"]["FORMATS"]["TXT"]:
            title_file = self.ctx.save_titles(results, id_to_name, failed_ids)
            print(f"Titles saved to: {title_file}")

        return results, id_to_name, failed_ids

    def _execute_mode_strategy(
        self, mode_strategy: Dict, results: Dict, id_to_name: Dict, failed_ids: List
    ) -> Optional[str]:
        """Execute mode-specific logic"""
        # Get current platform IDs
        current_platform_ids = self.ctx.platform_ids

        new_titles = self.ctx.detect_new_titles(current_platform_ids)
        time_info = self.ctx.format_time()
        if self.ctx.config["STORAGE"]["FORMATS"]["TXT"]:
            self.ctx.save_titles(results, id_to_name, failed_ids)
        word_groups, filter_words, global_filters = self.ctx.load_frequency_words()

        # In current mode, real-time push needs full historical data for complete statistics
        if self.report_mode == "current":
            # Load full historical data (filtered by current platforms)
            analysis_data = self._load_analysis_data()
            if analysis_data:
                (
                    all_results,
                    historical_id_to_name,
                    historical_title_info,
                    historical_new_titles,
                    _,
                    _,
                    _,
                ) = analysis_data

                print(
                    f"Current mode: Using filtered historical data, platforms: {list(all_results.keys())}"
                )

                stats, html_file = self._run_analysis_pipeline(
                    all_results,
                    self.report_mode,
                    historical_title_info,
                    historical_new_titles,
                    word_groups,
                    filter_words,
                    historical_id_to_name,
                    failed_ids=failed_ids,
                    global_filters=global_filters,
                )

                combined_id_to_name = {**historical_id_to_name, **id_to_name}

                if html_file:
                    print(f"HTML report generated: {html_file}")

                # Send real-time notification (using full historical statistics)
                summary_html = None
                if mode_strategy["should_send_realtime"]:
                    self._send_notification_if_needed(
                        stats,
                        mode_strategy["realtime_report_type"],
                        self.report_mode,
                        failed_ids=failed_ids,
                        new_titles=historical_new_titles,
                        id_to_name=combined_id_to_name,
                        html_file_path=html_file,
                    )
            else:
                print("❌ Critical error: Unable to read just-saved data file")
                raise RuntimeError("Data consistency check failed: read failed after save")
        else:
            title_info = self._prepare_current_title_info(results, time_info)
            stats, html_file = self._run_analysis_pipeline(
                results,
                self.report_mode,
                title_info,
                new_titles,
                word_groups,
                filter_words,
                id_to_name,
                failed_ids=failed_ids,
                global_filters=global_filters,
            )
            if html_file:
                print(f"HTML report generated: {html_file}")

            # Send real-time notification (if needed)
            summary_html = None
            if mode_strategy["should_send_realtime"]:
                self._send_notification_if_needed(
                    stats,
                    mode_strategy["realtime_report_type"],
                    self.report_mode,
                    failed_ids=failed_ids,
                    new_titles=new_titles,
                    id_to_name=id_to_name,
                    html_file_path=html_file,
                )

        # Generate summary report (if needed)
        summary_html = None
        if mode_strategy["should_generate_summary"]:
            if mode_strategy["should_send_realtime"]:
                # If real-time notification sent, summary only generates HTML
                summary_html = self._generate_summary_html(
                    mode_strategy["summary_mode"]
                )
            else:
                # Daily mode: generate summary report with notification
                summary_html = self._generate_summary_report(mode_strategy)

        # Open browser (only in non-container environment)
        if self._should_open_browser() and html_file:
            if summary_html:
                summary_url = "file://" + str(Path(summary_html).resolve())
                print(f"Opening summary report: {summary_url}")
                webbrowser.open(summary_url)
            else:
                file_url = "file://" + str(Path(html_file).resolve())
                print(f"Opening HTML report: {file_url}")
                webbrowser.open(file_url)
        elif self.is_docker_container and html_file:
            if summary_html:
                print(f"Summary report generated (Docker): {summary_html}")
            else:
                print(f"HTML report generated (Docker): {html_file}")

        return summary_html

    def run(self) -> None:
        """Execute analysis workflow"""
        try:
            self._initialize_and_check_config()

            mode_strategy = self._get_mode_strategy()

            results, id_to_name, failed_ids = self._crawl_data()

            self._execute_mode_strategy(mode_strategy, results, id_to_name, failed_ids)

        except Exception as e:
            print(f"Analysis workflow error: {e}")
            raise
        finally:
            # Cleanup resources (including expired data and database connections)
            self.ctx.cleanup()


def main():
    """Main program entry point"""
    try:
        analyzer = NewsAnalyzer()
        analyzer.run()
    except FileNotFoundError as e:
        print(f"❌ Config file error: {e}")
        print("\nPlease ensure these files exist:")
        print("  • config/config.yaml")
        print("  • config/frequency_words.txt")
        print("\nRefer to project documentation for setup")
    except Exception as e:
        print(f"❌ Program error: {e}")
        raise


if __name__ == "__main__":
    main()
